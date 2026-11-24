-- depends_on: {{ ref('int_orders_current') }}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    pre_hook="{{ remove_reclassified(false) }}"
) }}

with recent_orders as (
    select order_id
    from {{ ref('silver_orders_clean') }}
    {{ ingestion_window() }}
),

affected_orders as (
    select order_id from recent_orders
    {% if is_incremental() %}
        union
        -- Repair older facts if backfilled history splits a former interval.
        select f.order_id
        from {{ this }} as f
        left join {{ ref('dim_customers') }} as d
            on f.customer_version_id = d.customer_version_id
        where
            d.customer_version_id is null
            or f.created_at < d.valid_from
            or f.created_at >= d.valid_to
    {% endif %}
)

select
    o.order_id,
    o.customer_id,
    d.customer_version_id,
    o.product_id,
    o.order_status,
    o.quantity,
    o.unit_price,
    o.amount,
    o.created_at,
    o.updated_at,
    o.ingested_at,
    o.ingestion_date
from {{ ref('silver_orders_clean') }} as o
inner join affected_orders as a on o.order_id = a.order_id
left join {{ ref('dim_customers') }} as d
    on
        o.customer_id = d.customer_id
        and o.created_at >= d.valid_from
        and o.created_at < coalesce(d.valid_to, timestamptz 'infinity')
