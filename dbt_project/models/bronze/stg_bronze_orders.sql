select
    try_cast(source_row_id as bigint) as source_row_id,
    try_cast(order_id as bigint) as order_id,
    try_cast(customer_id as bigint) as customer_id,
    try_cast(product_id as bigint) as product_id,
    try_cast(order_status as varchar) as order_status,
    try_cast(quantity as integer) as quantity,
    try_cast(unit_price as decimal(18, 2)) as unit_price,
    try_cast(amount as decimal(18, 2)) as amount,
    try_cast(created_at as timestamptz) as created_at,
    try_cast(updated_at as timestamptz) as updated_at,
    try_cast(ingested_at as timestamptz) as ingested_at,
    try_cast(dt as date) as ingestion_date
from {{ raw_partition('orders') }}
