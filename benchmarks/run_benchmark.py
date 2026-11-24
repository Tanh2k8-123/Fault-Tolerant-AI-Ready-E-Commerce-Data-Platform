"""Compare monthly 30-calendar-day revenue queries against a built Gold warehouse."""

import argparse
import hashlib
import json
import logging
import os
import platform
import statistics
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from multiprocessing import get_context
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import psutil
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent


@dataclass
class Measurement:
    rows: list[tuple]
    seconds: float
    rss_mib: float
    plan: str


def measure(
    database: Path, parquet: Path, month: date, variant: str, explain: bool
) -> Measurement:
    """Use a fresh process per query. Exclude connection setup and plan capture from timing."""
    sql = (HERE / f"{variant}.sql").read_text(encoding="utf-8")
    with (
        TemporaryDirectory() as spill,
        duckdb.connect(
            str(database),
            read_only=True,
            config={"threads": 2, "memory_limit": "1GB", "temp_directory": spill},
        ) as db,
    ):
        db.execute("set TimeZone='UTC'")
        db.execute("set variable report_month = ?", [month])
        db.execute("set variable fact_glob = ?", [(parquet / "*/*.parquet").as_posix()])
        db.execute("""create temporary view benchmark_orders as
                   from read_parquet(getvariable('fact_glob'), hive_partitioning=true)""")
        process = psutil.Process()
        peak = process.memory_info().rss
        done = threading.Event()

        def sample() -> None:
            nonlocal peak
            while not done.wait(0.01):
                peak = max(peak, process.memory_info().rss)

        sampler = threading.Thread(target=sample)
        sampler.start()
        try:
            start = time.perf_counter()
            rows = db.execute(sql).fetchall()
            seconds = time.perf_counter() - start
            peak = max(peak, process.memory_info().rss)
        finally:
            done.set()
            sampler.join()
        plan = db.execute("explain analyze " + sql).fetchone()[1] if explain else ""
    return Measurement(rows, seconds, peak / 2**20, plan)


def benchmark(
    database: Path, parquet: Path, month: date, repeats: int, output: Path
) -> None:
    with duckdb.connect(str(database), read_only=True) as db:
        db.execute("set TimeZone='UTC'")
        db.execute("set threads=2")
        db.execute("set memory_limit='1GB'")
        db.execute("set variable export_path = ?", [str(parquet)])
        db.execute("""copy (
            select order_id, customer_id, customer_version_id, product_id, order_status,
                   quantity, unit_price, amount, created_at, updated_at, ingested_at,
                   ingestion_date, cast(created_at as date) as order_date
            from fct_orders
        ) to (getvariable('export_path')) (format parquet, partition_by (order_date))""")
        base = db.execute("select count(*) from int_orders_current").fetchone()[0]
        facts = db.execute("select count(*) from fct_orders").fetchone()[0]
        tiers = db.execute(
            "select count(distinct membership_tier) from dim_customers"
        ).fetchone()[0]
        days = db.execute("select day(last_day(?))", [month]).fetchone()[0]
    results: dict[str, list[Measurement]] = {"naive": [], "optimized": []}
    expected = None
    for i in range(repeats):
        for variant in tuple(results) if i % 2 == 0 else tuple(reversed(results)):
            with ProcessPoolExecutor(
                max_workers=1, mp_context=get_context("spawn")
            ) as worker:
                trial = worker.submit(
                    measure, database, parquet, month, variant, i == 0
                ).result()
            if expected is None:
                expected = trial.rows
            if (
                trial.rows != expected
                or len(trial.rows) != days * tiers
                or not trial.rows
            ):
                raise ValueError(
                    f"Unexpected result or mismatched rows for {variant}, trial {i + 1}"
                )
            results[variant].append(trial)

    digest = hashlib.sha256(json.dumps(expected, default=str).encode()).hexdigest()
    lines = [
        "# Rolling 30-day net revenue by membership tier",
        "",
        f"Reporting month {month:%Y-%m} UTC. Input contains {base:,} deduplicated "
        f"orders and {facts:,} valid Gold orders. Each of the {len(expected)} output rows "
        "sums completed order amounts in VND for the order date and preceding 29 calendar days.",
        "",
        f"Exact ordered results matched across all runs. SHA-256 `{digest}`.",
        "",
        f"DuckDB {duckdb.__version__}, Python {platform.python_version()}, {platform.system()} "
        f"{platform.machine()}, {psutil.virtual_memory().total / 2**30:.1f} GiB system RAM. "
        "Each query used two threads, a 1 GB DuckDB memory limit and a fresh process. "
        "Runs alternated order and shared the OS file cache. Wall time includes result fetching. "
        "Process RSS was sampled every 10 ms and includes interpreter and engine memory. "
        "Both variants read the same temporary Parquet export of Gold partitioned by UTC order date. "
        "The export was excluded from timing. Plans were captured in a separate execution after timing.",
        "",
        "| Query | Runs (seconds) | Median (seconds) | Highest sampled RSS (MiB) |",
        "|---|---|---:|---:|",
    ]
    for variant, trials in results.items():
        times = [x.seconds for x in trials]
        timings = ", ".join(f"{t:.3f}" for t in times)
        lines.append(
            f"| {variant} | {timings} | {statistics.median(times):.3f} | "
            f"{max(x.rss_mib for x in trials):.1f} |"
        )
    for variant, trials in results.items():
        lines.extend(
            [
                "",
                f"## {variant} EXPLAIN ANALYZE",
                "",
                "```text",
                trials[0].plan.strip(),
                "```",
            ]
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info("Wrote verified benchmark to %s", output)


def main() -> None:
    load_dotenv(Path.cwd() / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.getenv("DUCKDB_PATH", "data/ecommerce.duckdb")),
    )
    parser.add_argument("--month", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, default=HERE / "results.md")
    args = parser.parse_args()
    if args.month.day != 1 or args.repeats < 1:
        parser.error("Use the first day of a month and at least one repeat")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with TemporaryDirectory() as workspace:
        benchmark(
            args.database,
            Path(workspace) / "facts",
            args.month,
            args.repeats,
            args.output,
        )


if __name__ == "__main__":
    main()
