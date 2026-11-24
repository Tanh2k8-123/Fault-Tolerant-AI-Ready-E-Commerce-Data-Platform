{{ config(
    unique_key=["customer_id","ingestion_date"],
    merge_update_condition='(DBT_INTERNAL_SOURCE.updated_at, DBT_INTERNAL_SOURCE.ingested_at, DBT_INTERNAL_SOURCE.source_row_id) >= (DBT_INTERNAL_DEST.updated_at, DBT_INTERNAL_DEST.ingested_at, DBT_INTERNAL_DEST.source_row_id)'
) }}

with candidates as (
    select
        source_row_id,
        customer_id,
        membership_tier,
        created_at,
        updated_at,
        ingested_at,
        ingestion_date
    from {{ ref('stg_bronze_customers') }}
    {{ ingestion_window() }}
)

select
    source_row_id,
    customer_id,
    membership_tier,
    created_at,
    updated_at,
    ingested_at,
    ingestion_date
from candidates
qualify row_number() over (
    partition by customer_id, ingestion_date
    order by updated_at desc, ingested_at desc, source_row_id desc
) = 1
