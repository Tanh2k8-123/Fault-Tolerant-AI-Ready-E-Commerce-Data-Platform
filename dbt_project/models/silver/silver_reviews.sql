{{ config(
    unique_key="review_id",
    merge_update_condition='(DBT_INTERNAL_SOURCE.created_at, DBT_INTERNAL_SOURCE.ingested_at, DBT_INTERNAL_SOURCE.source_row_id) >= (DBT_INTERNAL_DEST.created_at, DBT_INTERNAL_DEST.ingested_at, DBT_INTERNAL_DEST.source_row_id)'
) }}

with candidates as (
    select
        source_row_id,
        review_id,
        order_id,
        customer_id,
        product_id,
        rating,
        review_text,
        created_at,
        ingested_at,
        ingestion_date
    from {{ ref('stg_bronze_reviews') }}
    {{ ingestion_window() }}
)

select
    source_row_id,
    review_id,
    order_id,
    customer_id,
    product_id,
    rating,
    {{ mask_review_pii('review_text') }} as review_text,
    created_at,
    ingested_at,
    ingestion_date
from candidates
qualify row_number() over (
    partition by review_id
    order by created_at desc, ingested_at desc, source_row_id desc
) = 1
