import hashlib
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.ingestion.chaos_generator import SCHEMAS, generate
from src.ingestion.config import FaultRates, GeneratorConfig


def checksums(root: Path) -> dict[Path, str]:
    return {
        p.relative_to(root): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def test_acceptance_and_retry(tmp_path: Path) -> None:
    config = GeneratorConfig(shard_size=127)
    raw = tmp_path / "first" / "raw"
    report = generate(config, raw)
    assert report["rows"] == dict(
        products=100, customers=1100, orders=10300, reviews=1000
    )
    assert report["faults"] == dict(
        duplicate=300, null_key=100, negative=50, late=500, pii=20
    )
    tables = {}
    for table, schema in SCHEMAS.items():
        records = []
        for path in (raw / table).glob("dt=*/*.parquet"):
            assert path.match("part-demo01-[0-9][0-9][0-9][0-9][0-9].parquet")
            data = pq.ParquetFile(path).read()
            assert data.schema == schema and data.num_rows <= 127
            rows = data.to_pylist()
            assert all(
                path.parent.name == f"dt={r['ingested_at'].date()}" for r in rows
            )
            records.extend(rows)
        assert sorted(r["source_row_id"] for r in records) == list(
            range(1, len(records) + 1)
        )
        tables[table] = records
    assert next((raw / "orders").rglob("*-00001.parquet"))

    groups = defaultdict(list)
    for row in tables["orders"]:
        groups[row["order_id"] or -row["source_row_id"]].append(row)
    assert len(groups) == 10000
    clean = {}
    late = Counter()
    statuses = Counter()
    errors = Counter()
    next_day = 0
    duplicates = 0
    for key, versions in groups.items():
        versions.sort(key=lambda r: r["updated_at"])
        first, last = versions[0], versions[-1]
        statuses[first["order_status"]] += 1
        days = (first["ingested_at"] - first["created_at"]).days
        late[days] += 1
        assert len(versions) in (1, 2)
        if len(versions) == 2:
            duplicates += 1
            delta = (last["ingested_at"].date() - first["ingested_at"].date()).days
            assert delta in (0, 1)
            next_day += delta
            assert (
                last["created_at"] == first["created_at"]
                and last["updated_at"] > first["updated_at"]
            )
            assert (
                last["quantity"] > first["quantity"]
                and last["amount"] == last["unit_price"] * last["quantity"]
            )
        invalid = (key < 0, last["customer_id"] is None, last["amount"] < 0)
        assert sum(invalid) + (len(versions) == 2) + bool(days) <= 1
        errors.update(dict(zip(("null_order", "null_customer", "negative"), invalid)))
        if not any(invalid):
            clean[key] = first
    assert duplicates == 300 and next_day == 150 and len(clean) == 9850
    assert errors == {"null_order": 50, "null_customer": 50, "negative": 50}
    assert late[0] == 9500 and set(late) == {0, 1, 2, 3}
    assert statuses == {"completed": 8500, "cancelled": 1000, "returned": 500}

    pii = Counter()
    for review in tables["reviews"]:
        order = clean[review["order_id"]]
        assert order["ingested_at"] <= review["ingested_at"]
        assert (order["customer_id"], order["product_id"]) == (
            review["customer_id"],
            review["product_id"],
        )
        assert 1 <= review["rating"] <= 5
        text = review["review_text"]
        pii[("@example.com" in text, "+1-202-555-" in text)] += 1
    assert pii == {
        (False, False): 980,
        (True, False): 8,
        (False, True): 8,
        (True, True): 4,
    }
    customers = tables["customers"]
    assert len({(r["customer_id"], r["updated_at"].date()) for r in customers}) == len(
        customers
    )

    expected = checksums(raw)
    second = tmp_path / "second"
    cmd = [
        sys.executable,
        "-m",
        "src.ingestion.chaos_generator",
        "--orders",
        "10000",
        "--reviews",
        "1000",
        "--output-dir",
        str(second),
        "--shard-size",
        "127",
        "--batch-id",
        "demo01",
        "--seed",
        "42",
    ]
    for _ in range(2):
        subprocess.run(cmd, check=True)
        assert checksums(second / "raw") == expected


def test_one_day_empty_reviews_and_batch_conflict(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    start = datetime(2024, 2, 29, tzinfo=UTC)
    args = dict(
        num_orders=20, num_days=1, num_reviews=0, start_date=start, num_customers=10
    )
    generate(GeneratorConfig(**args), raw)
    assert (
        pq.ParquetFile(next((raw / "reviews").rglob("*.parquet"))).metadata.num_rows
        == 0
    )
    assert len(list((raw / "customers").glob("dt=2024-02-29"))) == 1
    before = checksums(raw)
    with pytest.raises(FileExistsError):
        generate(GeneratorConfig(**args, random_seed=1), raw)
    assert before == checksums(raw)


def test_invalid_config() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(rates=FaultRates(duplicate=Decimal("0.99")))
    with pytest.raises(ValueError):
        GeneratorConfig(num_reviews=9851)
    with pytest.raises(ValueError):
        GeneratorConfig(batch_id="../escape")
