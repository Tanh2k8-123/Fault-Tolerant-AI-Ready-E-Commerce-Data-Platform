"""Generate deterministic raw e-commerce Parquet with NumPy and PyArrow."""

import argparse
import hashlib
import json
import logging
import math
import os
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


if __package__:
    from .config import GeneratorConfig
else:
    from config import GeneratorConfig
DAY = timedelta(days=1)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
ROW_GROUP = 8192
Row = dict[str, object]
ID = pa.int64()
TEXT = pa.string()
TIME = pa.timestamp("us", tz="UTC")
MONEY = pa.decimal128(18, 2)
STATUSES = ("completed", "cancelled", "returned")
TIERS = ("standard", "silver", "gold")
CLEAN, DUPLICATE, NULL_ORDER, NULL_CUSTOMER, NEGATIVE, LATE = range(6)


def schema(
    *fields: tuple[str, pa.DataType], nullable: tuple[str, ...] = ()
) -> pa.Schema:
    """Build a physical Parquet schema including shared provenance fields."""
    columns = (("source_row_id", ID), *fields, ("ingested_at", TIME))
    return pa.schema(
        [pa.field(name, typ, nullable=name in nullable) for name, typ in columns]
    )


SCHEMAS = {
    "orders": schema(
        ("order_id", ID),
        ("customer_id", ID),
        ("product_id", ID),
        ("quantity", pa.int32()),
        ("unit_price", MONEY),
        ("amount", MONEY),
        ("order_status", TEXT),
        ("created_at", TIME),
        ("updated_at", TIME),
        nullable=("order_id", "customer_id"),
    ),
    "customers": schema(
        ("customer_id", ID),
        ("membership_tier", TEXT),
        ("created_at", TIME),
        ("updated_at", TIME),
    ),
    "products": schema(
        ("product_id", ID),
        ("product_name", TEXT),
        ("category", TEXT),
        ("list_price", MONEY),
        ("updated_at", TIME),
    ),
    "reviews": schema(
        ("review_id", ID),
        ("order_id", ID),
        ("customer_id", ID),
        ("product_id", ID),
        ("rating", pa.int8()),
        ("review_text", TEXT),
        ("created_at", TIME),
    ),
}


@dataclass
class OrderPlan:
    faults: np.ndarray
    next_day: np.ndarray
    delay: np.ndarray
    status: np.ndarray
    customers: np.ndarray
    products: np.ndarray
    quantities: np.ndarray
    prices: np.ndarray
    config: GeneratorConfig


def plan_orders(config: GeneratorConfig, rng: np.random.Generator) -> OrderPlan:
    """Allocate exact disjoint cohorts and compact deterministic order attributes."""
    rows = config.num_orders
    rates = config.rates
    shuffled = rng.permutation(rows)
    duplicates = int(rows * rates.duplicate)
    nulls = int(rows * rates.null_key)
    late = int(rows * rates.late)
    faults = np.full(rows, CLEAN, dtype=np.uint8)
    next_day = np.zeros(rows, dtype=bool)
    next_day[shuffled[: duplicates // 2]] = True
    offset = 0
    for fault, count in (
        (DUPLICATE, duplicates),
        (NULL_ORDER, nulls // 2),
        (NULL_CUSTOMER, nulls - nulls // 2),
        (NEGATIVE, int(rows * rates.negative)),
        (LATE, late),
    ):
        faults[shuffled[offset : offset + count]] = fault
        offset += count
    delay = np.zeros(rows, dtype=np.uint8)
    late_ids = shuffled[offset - late : offset]
    delay[late_ids] = 1 + np.arange(len(late_ids)) % 3
    status = np.zeros(rows, dtype=np.uint8)
    status[: rows // 10] = 1
    status[rows // 10 : rows // 10 + rows // 20] = 2
    rng.shuffle(status)
    return OrderPlan(
        faults,
        next_day,
        delay,
        status,
        rng.integers(1, config.num_customers + 1, rows, dtype=np.int32),
        rng.integers(1, config.num_products + 1, rows, dtype=np.int32),
        rng.integers(1, 6, rows, dtype=np.uint8),
        rng.integers(10, 1001, config.num_products) * 1000,
        config,
    )


def order_time(index: int, config: GeneratorConfig) -> datetime:
    """Spread order creation across configured UTC dates, leaving room for same-day updates."""
    return (
        config.start_date
        + (index * config.num_days // config.num_orders) * DAY
        + (index % 1200 + 1) * MINUTE
    )


def orders(plan: OrderPlan) -> Iterator[Row]:
    """Emit base orders and revised versions with distinct physical row IDs."""
    source_id = 0
    for i, fault in enumerate(plan.faults):
        created = order_time(i, plan.config)
        product = int(plan.products[i])
        price = Decimal(int(plan.prices[product - 1]))
        quantity = int(plan.quantities[i])
        source_id += 1
        base = dict(
            source_row_id=source_id,
            order_id=None if fault == NULL_ORDER else i + 1,
            customer_id=None if fault == NULL_CUSTOMER else int(plan.customers[i]),
            product_id=product,
            order_status=STATUSES[plan.status[i]],
            quantity=quantity,
            unit_price=price,
            amount=price * (-quantity if fault == NEGATIVE else quantity),
            created_at=created,
            updated_at=created,
            ingested_at=created + int(plan.delay[i]) * DAY,
        )
        yield base
        if fault == DUPLICATE:
            updated = created + HOUR + int(plan.next_day[i]) * DAY
            source_id += 1
            yield dict(
                base,
                source_row_id=source_id,
                quantity=quantity + 1,
                amount=price * (quantity + 1),
                updated_at=updated,
                ingested_at=updated,
            )


def customers(config: GeneratorConfig) -> Iterator[Row]:
    """Emit initial profiles and a later tier change for every tenth customer."""
    source_id = 0
    for customer in range(1, config.num_customers + 1):
        tier = customer % len(TIERS)
        updated_dates = [config.start_date]
        if customer % 10 == 0 and config.num_days > 1:
            updated_dates.append(
                config.start_date + (1 + customer % (config.num_days - 1)) * DAY
            )
        for version, updated in enumerate(updated_dates):
            source_id += 1
            yield dict(
                source_row_id=source_id,
                customer_id=customer,
                created_at=config.start_date,
                membership_tier=TIERS[(tier + version) % len(TIERS)],
                updated_at=updated,
                ingested_at=updated,
            )


def products(prices: np.ndarray, start: datetime) -> Iterator[Row]:
    """Emit the catalog used to price and reference orders."""
    categories = ("home", "books", "electronics")
    for product, price in enumerate(prices, start=1):
        yield dict(
            source_row_id=product,
            product_id=product,
            product_name=f"Product {product}",
            category=categories[(product - 1) % len(categories)],
            list_price=Decimal(int(price)),
            updated_at=start,
            ingested_at=start,
        )


def reviews(plan: OrderPlan, count: int, rng: np.random.Generator) -> Iterator[Row]:
    """Sample eligible orders and inject synthetic email and phone fixtures into text."""
    eligible = np.flatnonzero(
        ~np.isin(plan.faults, (NULL_ORDER, NULL_CUSTOMER, NEGATIVE))
    )
    selected = rng.choice(eligible, size=count, replace=False)
    pii = int(count * plan.config.rates.pii)
    single = pii * 2 // 5
    for i, index in enumerate(selected):
        created = (
            order_time(int(index), plan.config) + int(plan.delay[index]) * DAY + HOUR
        )
        body = "The product matched the description."
        if i < single or 2 * single <= i < pii:
            body += f" Email: buyer{i + 1}@example.com."
        if single <= i < pii:
            body += f" Phone: +1-202-555-01{i % 100:02d}."
        yield dict(
            source_row_id=i + 1,
            review_id=i + 1,
            order_id=int(index) + 1,
            customer_id=int(plan.customers[index]),
            product_id=int(plan.products[index]),
            rating=int(rng.integers(1, 6)),
            review_text=body,
            created_at=created,
            ingested_at=created,
        )


def write_table(
    root: Path, table: str, rows: Iterable[Row], batch: str, shard_size: int
) -> int:
    """Route rows by arrival date and stream bounded row groups into numbered shards."""
    buffers: dict[date, list[Row]] = {}
    counts: dict[date, int] = {}
    writers: dict[date, pq.ParquetWriter] = {}

    def flush(day: date) -> None:
        """Append one buffered row group to the active shard for this UTC date."""
        if day not in writers:
            folder = root / table / f"dt={day.isoformat()}"
            folder.mkdir(parents=True, exist_ok=True)
            shard = counts[day] // shard_size
            path = folder / f"part-{batch}-{shard:05d}.parquet"
            writers[day] = pq.ParquetWriter(path, SCHEMAS[table], compression="zstd")
        group = buffers[day]
        writers[day].write_table(pa.Table.from_pylist(group, schema=SCHEMAS[table]))
        counts[day] += len(group)
        group.clear()
        if counts[day] % shard_size == 0:
            writers.pop(day).close()

    try:
        for row in rows:
            arrival = row["ingested_at"]
            assert isinstance(arrival, datetime)
            day = arrival.date()
            group = buffers.setdefault(day, [])
            group.append(row)
            remaining = shard_size - counts.setdefault(day, 0) % shard_size
            if len(group) == min(ROW_GROUP, remaining):
                flush(day)
        for day, group in buffers.items():
            if group:
                flush(day)
    finally:
        for writer in writers.values():
            writer.close()
    return sum(counts.values())


def publish(staging: Path, output: Path) -> None:
    """Publish completed files without overwriting and verify identical retries."""
    for source in sorted(path for path in staging.rglob("*") if path.is_file()):
        target = output / source.relative_to(staging)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source, target)
        except FileExistsError:
            with source.open("rb") as new, target.open("rb") as old:
                if (
                    hashlib.file_digest(new, "sha256").digest()
                    != hashlib.file_digest(old, "sha256").digest()
                ):
                    raise FileExistsError(f"Checksum mismatch: {target}") from None


def generate(config: GeneratorConfig, raw_dir: Path) -> dict[str, dict[str, int]]:
    """Write a reproducible batch and count faults from the emitted records."""
    rng = np.random.default_rng(config.random_seed)
    plan = plan_orders(config, rng)
    totals = {}
    faults = dict.fromkeys(("duplicate", "null_key", "negative", "late", "pii"), 0)

    def observed(table: str, records: Iterable[Row]) -> Iterator[Row]:
        for row in records:
            if table == "orders":
                faults["duplicate"] += row["updated_at"] != row["created_at"]
                faults["null_key"] += (
                    row["order_id"] is None or row["customer_id"] is None
                )
                faults["negative"] += row["amount"] < 0
                faults["late"] += row["ingested_at"] > row["updated_at"]
            elif table == "reviews":
                faults["pii"] += (
                    " Email:" in row["review_text"] or " Phone:" in row["review_text"]
                )
            yield row

    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=raw_dir.parent) as tmp:
        staging = Path(tmp)
        for table, records in (
            ("products", products(plan.prices, config.start_date)),
            ("customers", customers(config)),
            ("orders", orders(plan)),
            ("reviews", reviews(plan, config.num_reviews, rng)),
        ):
            totals[table] = write_table(
                staging,
                table,
                observed(table, records),
                config.batch_id,
                config.shard_size,
            )
            if totals[table] == 0:
                folder = staging / table / f"dt={config.start_date.date()}"
                folder.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    pa.Table.from_pylist([], schema=SCHEMAS[table]),
                    folder / f"part-{config.batch_id}-00000.parquet",
                    compression="zstd",
                )
        # Reserve this batch's settings before publishing shards, including on concurrent retries.
        manifest = staging / ".batches" / f"{config.batch_id}.json"
        manifest.parent.mkdir()
        manifest.write_bytes(
            json.dumps(asdict(config), sort_keys=True, default=str).encode()
        )
        publish(staging, raw_dir)
    return {"rows": totals, "faults": faults}


def main() -> None:
    """Parse CLI options, generate all four tables, and report measured counts."""
    defaults = GeneratorConfig()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", "--rows", type=int, default=defaults.num_orders)
    parser.add_argument("--reviews", type=int, help="default: orders // 10")
    parser.add_argument("--customers", type=int, help="default: max(1, orders // 10)")
    parser.add_argument("--products", type=int, help="default: max(1, isqrt(orders))")
    parser.add_argument("--num-days", type=int, default=defaults.num_days)
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=defaults.start_date.date()
    )
    parser.add_argument("--batch-id", default=defaults.batch_id)
    parser.add_argument("--seed", type=int, default=defaults.random_seed)
    parser.add_argument("--shard-size", type=int, default=defaults.shard_size)
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument(
        "--output-dir", type=Path, default=Path("data"), help="parent of raw/"
    )
    paths.add_argument("--output", type=Path, help="legacy direct path to raw/")
    args = parser.parse_args()
    try:
        config = GeneratorConfig(
            random_seed=args.seed,
            batch_id=args.batch_id,
            num_orders=args.orders,
            num_customers=args.customers
            if args.customers is not None
            else max(1, args.orders // 10),
            num_products=args.products
            if args.products is not None
            else max(1, math.isqrt(args.orders)),
            num_reviews=args.reviews if args.reviews is not None else args.orders // 10,
            start_date=datetime.combine(args.start_date, datetime.min.time(), UTC),
            num_days=args.num_days,
            shard_size=args.shard_size,
        )
    except ValueError as exc:
        parser.error(str(exc))
    raw_dir = args.output or args.output_dir / "raw"
    report = generate(config, raw_dir)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for group, counts in report.items():
        for name, count in counts.items():
            logging.info("%-8s %-12s %10d", group, name, count)


if __name__ == "__main__":
    main()
