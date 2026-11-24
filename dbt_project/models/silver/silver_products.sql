{{ config(
    unique_key="product_id",
    merge_update_condition='(DBT_INTERNAL_SOURCE.updated_at, DBT_INTERNAL_SOURCE.ingested_at, DBT_INTERNAL_SOURCE.source_row_id) >= (DBT_INTERNAL_DEST.updated_at, DBT_INTERNAL_DEST.ingested_at, DBT_INTERNAL_DEST.source_row_id)'
) }}

with candidates as (
    select
        source_row_id,
        product_id,
        product_name,
        category,
        list_price,
        updated_at,
        ingested_at,
        ingestion_date
    from {{ ref('stg_bronze_products') }}
    {{ ingestion_window() }}
)

select
    source_row_id,
    product_id,
    product_name,
    category,
    list_price,
    updated_at,
    ingested_at,
    ingestion_date
from candidates
qualify row_number() over (
    partition by product_id
    order by updated_at desc, ingested_at desc, source_row_id desc
) = 1
