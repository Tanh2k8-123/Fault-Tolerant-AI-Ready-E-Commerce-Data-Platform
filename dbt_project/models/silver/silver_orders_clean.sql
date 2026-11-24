{{ config(unique_key='order_id', pre_hook="{{ remove_reclassified(false) }}") }}

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
    ingestion_date
from candidates
where error_reason is null
