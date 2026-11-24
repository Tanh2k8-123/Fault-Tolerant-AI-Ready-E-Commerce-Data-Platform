{{ config(
    unique_key='source_row_id',
    pre_hook="{{ remove_reclassified(true) }}"
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
        error_reason
    from {{ ref('int_orders_current') }}
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
    error_reason,
    ingested_at as quarantined_at
from candidates
where error_reason is not null
