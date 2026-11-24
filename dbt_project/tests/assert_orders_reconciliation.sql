with published as (
    select amount, ingestion_date from {{ ref('fct_orders') }}
    union all
    select amount, ingestion_date from {{ ref('quarantine_orders') }}
),

latest_bronze as (
    select amount
    from {{ ref('stg_bronze_orders') }}
    {% if var('partition_date', none) is not none %}
        -- Replaying an earlier day does not rewind materialized facts.
        where ingestion_date <= greatest(
            date '{{ modules.datetime.date.fromisoformat(var("partition_date") | string) }}',
            (select max(ingestion_date) from published)
        )
    {% endif %}
    qualify row_number() over (
        partition by order_id is null, coalesce(order_id, source_row_id)
        order by updated_at desc, ingested_at desc, source_row_id desc
    ) = 1
),

expected as (
    select count(*) as rows_expected, coalesce(sum(amount), 0) as amount_expected
    from latest_bronze
),

actual as (
    select count(*) as rows_actual, coalesce(sum(amount), 0) as amount_actual
    from published
)

select rows_expected, rows_actual, amount_expected, amount_actual
from expected cross join actual
where rows_expected <> rows_actual or amount_expected <> amount_actual
