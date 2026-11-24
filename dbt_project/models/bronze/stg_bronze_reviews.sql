select
    try_cast(source_row_id as bigint) as source_row_id,
    try_cast(review_id as bigint) as review_id,
    try_cast(order_id as bigint) as order_id,
    try_cast(customer_id as bigint) as customer_id,
    try_cast(product_id as bigint) as product_id,
    try_cast(rating as tinyint) as rating,
    try_cast(review_text as varchar) as review_text,
    try_cast(created_at as timestamptz) as created_at,
    try_cast(ingested_at as timestamptz) as ingested_at,
    try_cast(dt as date) as ingestion_date
from {{ raw_partition('reviews') }}
