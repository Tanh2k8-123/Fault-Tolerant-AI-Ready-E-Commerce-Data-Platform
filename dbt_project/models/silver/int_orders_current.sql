{{ config(
    unique_key=['missing_order_id', 'record_key'],
    merge_update_condition='(DBT_INTERNAL_SOURCE.updated_at, DBT_INTERNAL_SOURCE.ingested_at, DBT_INTERNAL_SOURCE.source_row_id) >= (DBT_INTERNAL_DEST.updated_at, DBT_INTERNAL_DEST.ingested_at, DBT_INTERNAL_DEST.source_row_id)'
) }}

with candidates as (
    select
        source_row_id,
        order_id,
        customer_id,
        product_id,
        order_status,
        quantity,
        unit_price,
        amount,
        created_at,
        updated_at,
        ingested_at,
        ingestion_date,
        order_id is null as missing_order_id,
        coalesce(order_id, source_row_id) as record_key
    from {{ ref('stg_bronze_orders') }}
    {{ ingestion_window() }}
)

select
    source_row_id,
    order_id,
    customer_id,
    product_id,
    order_status,
    quantity,
    unit_price,
    amount,
    created_at,
    updated_at,
    ingested_at,
    ingestion_date,
    missing_order_id,
    record_key,
    case
        when order_id is null then 'null_order_id'
        when customer_id is null then 'null_customer_id'
        when amount < 0 then 'negative_amount'
    end as error_reason
from candidates
qualify row_number() over (
    partition by missing_order_id, record_key
    order by updated_at desc, ingested_at desc, source_row_id desc
) = 1
