from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from ai_serving.text_to_sql_demo import (
    EXAMPLES,
    generate_sql,
    metadata,
    query,
    validated_sql,
)

ROOT = Path(__file__).resolve().parents[1]


def test_rolling_calendar() -> None:
    with duckdb.connect() as db:
        db.execute("set TimeZone='UTC'")
        db.execute("set variable report_month = date '2026-01-01'")
        db.execute(
            "create table dim_customers(customer_version_id varchar, membership_tier varchar)"
        )
        db.execute("insert into dim_customers values ('old','silver'), ('new','gold')")
        db.execute(
            "create table fct_orders(created_at timestamptz, customer_version_id varchar, "
            "amount decimal(18,2), order_status varchar)"
        )
        db.execute("""insert into fct_orders values
            ('2025-12-02','old',100,'completed'), ('2025-12-03','old',5,'completed'),
            ('2026-01-01','new',7,'completed'), ('2026-01-01','new',30,'returned'),
            ('2026-01-02','new',40,'cancelled')""")
        db.execute("""create view benchmark_orders as
            select created_at, customer_version_id, amount, order_status,
                   cast(created_at as date) as order_date from fct_orders""")
        for variant in ("naive", "optimized"):
            sql = (ROOT / "benchmarks" / f"{variant}.sql").read_text(encoding="utf-8")
            rows = db.execute(sql).fetchall()
            assert len(rows) == 62
            for day, tier, amount in rows:
                expected = 5 if tier == "silver" and day.day == 1 else 0
                if tier == "gold" and day.day <= 30:
                    expected = 7
                assert amount == Decimal(expected)


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE fct_orders",
        "DELETE FROM fct_orders",
        "UPDATE fct_orders SET amount = 0",
        "SELECT amount FROM fct_orders; DELETE FROM fct_orders",
        "SELECT order_id FROM stg_bronze_orders",
        "SELECT version() FROM fct_orders",
    ],
)
def test_read_only_policy(sql: str) -> None:
    with pytest.raises(ValueError):
        validated_sql(sql)


def test_offline_metadata_and_execution(tmp_path: Path) -> None:
    path = tmp_path / "gold.duckdb"
    with duckdb.connect(str(path)) as db:
        db.execute(
            "create table mart_daily_revenue(order_date date, membership_tier varchar, "
            "gmv decimal(18,2), net_revenue decimal(18,2))"
        )
        db.execute(
            "insert into mart_daily_revenue values (date '2026-01-01','silver',20,12)"
        )
        db.execute("create table fct_orders(order_id bigint)")
        db.execute("insert into fct_orders values (1)")
    context = metadata(ROOT / "dbt_project/models/gold/schema.yml")
    for question, expected in zip(
        EXAMPLES,
        [
            [("silver", Decimal(12))],
            [(date(2026, 1, 1), Decimal(20))],
            [(1,)],
        ],
        strict=True,
    ):
        answer = query(path, generate_sql(question, context, None))
        assert answer["rows"] == expected and not answer["truncated"]
