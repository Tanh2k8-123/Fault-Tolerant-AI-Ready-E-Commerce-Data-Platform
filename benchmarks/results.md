# Rolling 30-day net revenue by membership tier

Reporting month 2026-03 UTC. Input contains 5,000,000 deduplicated orders and 4,925,000 valid Gold orders. Each of the 93 output rows sums completed order amounts in VND for the order date and preceding 29 calendar days.

Exact ordered results matched across all runs. SHA-256 `dad3ced69ede7bebe3308d4123edbf0c10bd79b00d2394a433c07a25d65c5618`.

DuckDB 1.4.3, Python 3.12.10, Windows AMD64, 15.7 GiB system RAM. Each query used two threads, a 1 GB DuckDB memory limit and a fresh process. Runs alternated order and shared the OS file cache. Wall time includes result fetching. Process RSS was sampled every 10 ms and includes interpreter and engine memory. Both variants read the same temporary Parquet export of Gold partitioned by UTC order date. The export was excluded from timing. Plans were captured in a separate execution after timing.

| Query | Runs (seconds) | Median (seconds) | Highest sampled RSS (MiB) |
|---|---|---:|---:|
| naive | 14.128, 4.033, 4.493 | 4.493 | 857.9 |
| optimized | 1.050, 0.980, 0.950 | 0.980 | 238.0 |

The baseline read all 600 fact files. The optimized query read 300 across the 60 required UTC dates (January 31 through March 31), and projected only the values needed for the join and aggregation. The same decimal-valued result ran 4.58 times as fast by median wall time, with 72% lower peak sampled process RSS in this run.

## Reproduce the 5-million-order sample

Run these PowerShell commands from the repository root with the project requirements installed. The generator uses seed 42 and batch `demo01`. A separate database keeps the benchmark independent of the Airflow warehouse.

```powershell
python src/ingestion/chaos_generator.py --orders 5000000 --num-days 120 --output-dir data/benchmark-5m
$env:RAW_DIR = 'data/benchmark-5m/raw'
$env:DUCKDB_PATH = 'data/benchmark-5m/benchmark.duckdb'
dbt build --target local --project-dir dbt_project --profiles-dir dbt_project
python benchmarks/run_benchmark.py --month 2026-03-01 --output data/benchmark-5m/results.md
```

## naive EXPLAIN ANALYZE

```text
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
explain analyze -- Deliberately materialize the wide historical join for the baseline. with joined as materialized (     select *     from benchmark_orders     inner join dim_customers using (customer_version_id) ),  daily as (     select         cast(created_at as date) as order_date,         membership_tier,         sum(amount) as net_revenue     from joined     where order_status = 'completed'     group by cast(created_at as date), membership_tier ),  calendar as (     select cast(day as date) as order_date     from generate_series(         getvariable('report_month') - interval '29 days',         getvariable('report_month') + interval '1 month' - interval '1 day',         interval '1 day'     ) as dates(day) ),  tiers as (     select distinct membership_tier from dim_customers ),  rolling as (     select         c.order_date,         t.membership_tier,         sum(coalesce(d.net_revenue, 0)) over (             partition by t.membership_tier order by c.order_date             rows between 29 preceding and current row         ) as net_revenue_30d     from calendar as c     cross join tiers as t     left join daily as d         on c.order_date = d.order_date and t.membership_tier = d.membership_tier )  select order_date, membership_tier, net_revenue_30d from rolling where     year(order_date) = year(getvariable('report_month'))     and month(order_date) = month(getvariable('report_month')) order by order_date, membership_tier
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││               Total Time: 7.02s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌───────────────────────────┐
│           QUERY           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      EXPLAIN_ANALYZE      │
│    ────────────────────   │
│           0 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│            CTE            │
│    ────────────────────   │
│      CTE Name: joined     │
│       Table Index: 0      ├───────────────────────────────────────────┐
│                           │                                           │
│           0 rows          │                                           │
│          (1.97s)          │                                           │
└─────────────┬─────────────┘                                           │
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         HASH_JOIN         │                             │         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   │
│      Join Type: INNER     │                             │             #0            │
│                           │                             │__internal_decompress_strin│
│        Conditions:        │                             │           g(#1)           │
│   customer_version_id =   ├──────────────┐              │             #2            │
│     customer_version_id   │              │              │                           │
│                           │              │              │                           │
│       4,186,283 rows      │              │              │          93 rows          │
│          (3.29s)          │              │              │          (0.00s)          │
└─────────────┬─────────────┘              │              └─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         TABLE_SCAN        ││         TABLE_SCAN        ││          ORDER_BY         │
│    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
│         Function:         ││    Table: dim_customers   ││   rolling.order_date ASC  │
│        READ_PARQUET       ││   Type: Sequential Scan   ││rolling.membership_tier ASC│
│                           ││                           ││                           │
│        Projections:       ││        Projections:       ││                           │
│          order_id         ││    customer_version_id    ││                           │
│        customer_id        ││        customer_id        ││                           │
│    customer_version_id    ││      membership_tier      ││                           │
│         product_id        ││         valid_from        ││                           │
│        order_status       ││          valid_to         ││                           │
│          quantity         ││         is_current        ││                           │
│         unit_price        ││                           ││                           │
│           amount          ││                           ││                           │
│         created_at        ││                           ││                           │
│         updated_at        ││                           ││                           │
│        ingested_at        ││                           ││                           │
│       ingestion_date      ││                           ││                           │
│         order_date        ││                           ││                           │
│                           ││                           ││                           │
│          Filters:         ││                           ││                           │
│  order_status='completed' ││                           ││                           │
│                           ││                           ││                           │
│   Total Files Read: 600   ││                           ││                           │
│                           ││                           ││                           │
│       4,186,283 rows      ││        550,000 rows       ││          93 rows          │
│          (5.89s)          ││          (0.06s)          ││          (0.00s)          │
└───────────────────────────┘└───────────────────────────┘└─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │         PROJECTION        │
                                                          │    ────────────────────   │
                                                          │             #0            │
                                                          │__internal_compress_string_│
                                                          │        uhugeint(#1)       │
                                                          │             #2            │
                                                          │                           │
                                                          │          93 rows          │
                                                          │          (0.00s)          │
                                                          └─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │           FILTER          │
                                                          │    ────────────────────   │
                                                          │   (("year"(order_date) =  │
                                                          │     2026) AND ("month"    │
                                                          │     (order_date) = 3))    │
                                                          │                           │
                                                          │          93 rows          │
                                                          │          (0.00s)          │
                                                          └─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │         PROJECTION        │
                                                          │    ────────────────────   │
                                                          │         order_date        │
                                                          │      membership_tier      │
                                                          │      net_revenue_30d      │
                                                          │                           │
                                                          │          180 rows         │
                                                          │          (0.00s)          │
                                                          └─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │         PROJECTION        │
                                                          │    ────────────────────   │
                                                          │             #0            │
                                                          │             #1            │
                                                          │             #2            │
                                                          │             #3            │
                                                          │                           │
                                                          │          180 rows         │
                                                          │          (0.00s)          │
                                                          └─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │           WINDOW          │
                                                          │    ────────────────────   │
                                                          │        Projections:       │
                                                          │ sum(COALESCE(net_revenue, │
                                                          │  0.00)) OVER (PARTITION BY│
                                                          │  membership_tier ORDER BY │
                                                          │  order_date ASC NULLS LAST│
                                                          │  ROWS BETWEEN 29 PRECEDING│
                                                          │      AND CURRENT ROW)     │
                                                          │                           │
                                                          │          180 rows         │
                                                          │          (0.01s)          │
                                                          └─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │         HASH_JOIN         │
                                                          │    ────────────────────   │
                                                          │      Join Type: RIGHT     │
                                                          │                           │
                                                          │        Conditions:        │
                                                          │  order_date = order_date  ├──────────────┐
                                                          │     membership_tier =     │              │
                                                          │       membership_tier     │              │
                                                          │                           │              │
                                                          │          180 rows         │              │
                                                          │          (0.00s)          │              │
                                                          └─────────────┬─────────────┘              │
                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                          │       HASH_GROUP_BY       ││       CROSS_PRODUCT       │
                                                          │    ────────────────────   ││    ────────────────────   │
                                                          │          Groups:          ││                           │
                                                          │             #0            ││                           │
                                                          │             #1            ││                           │
                                                          │                           ││                           ├──────────────┐
                                                          │    Aggregates: sum(#2)    ││                           │              │
                                                          │                           ││                           │              │
                                                          │          360 rows         ││          180 rows         │              │
                                                          │          (0.37s)          ││          (0.00s)          │              │
                                                          └─────────────┬─────────────┘└─────────────┬─────────────┘              │
                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                          │         PROJECTION        ││         PROJECTION        ││         PROJECTION        │
                                                          │    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
                                                          │  CAST(created_at AS DATE) ││__internal_decompress_strin││         order_date        │
                                                          │      membership_tier      ││           g(#0)           ││                           │
                                                          │           amount          ││                           ││                           │
                                                          │                           ││                           ││                           │
                                                          │       4,186,283 rows      ││           3 rows          ││          60 rows          │
                                                          │          (1.95s)          ││          (0.00s)          ││          (0.00s)          │
                                                          └─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                          │         PROJECTION        ││       HASH_GROUP_BY       ││         TABLE_SCAN        │
                                                          │    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
                                                          │             #7            ││         Groups: #0        ││         Function:         │
                                                          │             #8            ││                           ││      GENERATE_SERIES      │
                                                          │            #14            ││                           ││                           │
                                                          │                           ││                           ││                           │
                                                          │       4,186,283 rows      ││           3 rows          ││          60 rows          │
                                                          │          (0.00s)          ││          (0.00s)          ││          (0.00s)          │
                                                          └─────────────┬─────────────┘└─────────────┬─────────────┘└───────────────────────────┘
                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                          │           FILTER          ││         PROJECTION        │
                                                          │    ────────────────────   ││    ────────────────────   │
                                                          │ (order_status = 'completed││      membership_tier      │
                                                          │             ')            ││                           │
                                                          │                           ││                           │
                                                          │       4,186,283 rows      ││        550,000 rows       │
                                                          │          (0.05s)          ││          (0.00s)          │
                                                          └─────────────┬─────────────┘└─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                          │          CTE_SCAN         ││         PROJECTION        │
                                                          │    ────────────────────   ││    ────────────────────   │
                                                          │        CTE Index: 0       ││__internal_compress_string_│
                                                          │                           ││        uhugeint(#0)       │
                                                          │                           ││                           │
                                                          │       4,186,283 rows      ││        550,000 rows       │
                                                          │          (0.06s)          ││          (0.00s)          │
                                                          └───────────────────────────┘└─────────────┬─────────────┘
                                                                                       ┌─────────────┴─────────────┐
                                                                                       │         TABLE_SCAN        │
                                                                                       │    ────────────────────   │
                                                                                       │    Table: dim_customers   │
                                                                                       │   Type: Sequential Scan   │
                                                                                       │                           │
                                                                                       │        Projections:       │
                                                                                       │      membership_tier      │
                                                                                       │                           │
                                                                                       │        550,000 rows       │
                                                                                       │          (0.00s)          │
                                                                                       └───────────────────────────┘
```

## optimized EXPLAIN ANALYZE

```text
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
explain analyze with filtered as (     select order_date, customer_version_id, amount     from benchmark_orders     where         order_status = 'completed'         and order_date >= getvariable('report_month') - 29         and order_date < cast(getvariable('report_month') + interval '1 month' as date) ),  daily as (     select         o.order_date,         d.membership_tier,         sum(o.amount) as net_revenue     from filtered as o     inner join dim_customers as d         on o.customer_version_id = d.customer_version_id     group by order_date, d.membership_tier ),  calendar as (     select cast(day as date) as order_date     from generate_series(         getvariable('report_month') - interval '29 days',         getvariable('report_month') + interval '1 month' - interval '1 day',         interval '1 day'     ) as dates(day) ),  tiers as (     select distinct membership_tier from dim_customers ),  rolling as (     select         c.order_date,         t.membership_tier,         sum(coalesce(d.net_revenue, 0)) over (             partition by t.membership_tier order by c.order_date             rows between 29 preceding and current row         ) as net_revenue_30d     from calendar as c     cross join tiers as t     left join daily as d         on c.order_date = d.order_date and t.membership_tier = d.membership_tier )  select order_date, membership_tier, net_revenue_30d from rolling where order_date >= getvariable('report_month') order by order_date, membership_tier
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││              Total Time: 0.937s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌───────────────────────────┐
│           QUERY           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      EXPLAIN_ANALYZE      │
│    ────────────────────   │
│           0 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│__internal_decompress_strin│
│           g(#1)           │
│             #2            │
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│          ORDER_BY         │
│    ────────────────────   │
│   rolling.order_date ASC  │
│rolling.membership_tier ASC│
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│__internal_compress_string_│
│        uhugeint(#1)       │
│             #2            │
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│         order_date        │
│      membership_tier      │
│      net_revenue_30d      │
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│             #2            │
│             #3            │
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│ (order_date >= '2026-03-01│
│          '::DATE)         │
│                           │
│          93 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│                           │
│          180 rows         │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           WINDOW          │
│    ────────────────────   │
│        Projections:       │
│ sum(COALESCE(net_revenue, │
│  0.00)) OVER (PARTITION BY│
│  membership_tier ORDER BY │
│  order_date ASC NULLS LAST│
│  ROWS BETWEEN 29 PRECEDING│
│      AND CURRENT ROW)     │
│                           │
│          180 rows         │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         HASH_JOIN         │
│    ────────────────────   │
│      Join Type: RIGHT     │
│                           │
│        Conditions:        │
│  order_date = order_date  ├───────────────────────────────────────────┐
│     membership_tier =     │                                           │
│       membership_tier     │                                           │
│                           │                                           │
│          180 rows         │                                           │
│          (0.00s)          │                                           │
└─────────────┬─────────────┘                                           │
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         PROJECTION        │                             │       CROSS_PRODUCT       │
│    ────────────────────   │                             │    ────────────────────   │
│             #0            │                             │                           │
│__internal_decompress_strin│                             │                           │
│           g(#1)           │                             │                           ├──────────────┐
│             #2            │                             │                           │              │
│                           │                             │                           │              │
│          180 rows         │                             │          180 rows         │              │
│          (0.00s)          │                             │          (0.00s)          │              │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘              │
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│       HASH_GROUP_BY       │                             │         PROJECTION        ││         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   ││    ────────────────────   │
│          Groups:          │                             │__internal_decompress_strin││         order_date        │
│             #0            │                             │           g(#0)           ││                           │
│             #1            │                             │                           ││                           │
│                           │                             │                           ││                           │
│    Aggregates: sum(#2)    │                             │                           ││                           │
│                           │                             │                           ││                           │
│          180 rows         │                             │           3 rows          ││          60 rows          │
│          (0.09s)          │                             │          (0.00s)          ││          (0.00s)          │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         PROJECTION        │                             │       HASH_GROUP_BY       ││         TABLE_SCAN        │
│    ────────────────────   │                             │    ────────────────────   ││    ────────────────────   │
│         order_date        │                             │         Groups: #0        ││         Function:         │
│      membership_tier      │                             │                           ││      GENERATE_SERIES      │
│           amount          │                             │                           ││                           │
│                           │                             │                           ││                           │
│       2,092,523 rows      │                             │           3 rows          ││          60 rows          │
│          (0.00s)          │                             │          (0.00s)          ││          (0.00s)          │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘└───────────────────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         PROJECTION        │                             │         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   │
│__internal_compress_string_│                             │      membership_tier      │
│        uhugeint(#0)       │                             │                           │
│             #1            │                             │                           │
│             #2            │                             │                           │
│                           │                             │                           │
│       2,092,523 rows      │                             │        550,000 rows       │
│          (0.00s)          │                             │          (0.00s)          │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         HASH_JOIN         │                             │         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   │
│      Join Type: INNER     │                             │__internal_compress_string_│
│                           │                             │        uhugeint(#0)       │
│        Conditions:        │                             │                           │
│   customer_version_id =   ├──────────────┐              │                           │
│     customer_version_id   │              │              │                           │
│                           │              │              │                           │
│       2,092,523 rows      │              │              │        550,000 rows       │
│          (0.89s)          │              │              │          (0.00s)          │
└─────────────┬─────────────┘              │              └─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         TABLE_SCAN        ││         PROJECTION        ││         TABLE_SCAN        │
│    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
│    Table: dim_customers   ││         order_date        ││    Table: dim_customers   │
│   Type: Sequential Scan   ││    customer_version_id    ││   Type: Sequential Scan   │
│                           ││           amount          ││                           │
│        Projections:       ││                           ││        Projections:       │
│    customer_version_id    ││                           ││      membership_tier      │
│      membership_tier      ││                           ││                           │
│                           ││                           ││                           │
│        549,999 rows       ││       2,092,523 rows      ││        550,000 rows       │
│          (0.05s)          ││          (0.00s)          ││          (0.00s)          │
└───────────────────────────┘└─────────────┬─────────────┘└───────────────────────────┘
                             ┌─────────────┴─────────────┐
                             │         TABLE_SCAN        │
                             │    ────────────────────   │
                             │         Function:         │
                             │        READ_PARQUET       │
                             │                           │
                             │        Projections:       │
                             │    customer_version_id    │
                             │           amount          │
                             │         order_date        │
                             │                           │
                             │          Filters:         │
                             │  order_status='completed' │
                             │                           │
                             │       File Filters:       │
                             │ (order_date BETWEEN '2026 │
                             │ -01-31'::DATE AND '2026-04│
                             │        -01'::DATE)        │
                             │                           │
                             │      Scanning Files:      │
                             │          300/600          │
                             │                           │
                             │   Total Files Read: 300   │
                             │                           │
                             │       2,092,523 rows      │
                             │          (0.66s)          │
                             └───────────────────────────┘
```
