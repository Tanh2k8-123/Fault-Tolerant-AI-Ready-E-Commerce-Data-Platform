select
    try_cast(source_row_id as bigint) as source_row_id,
    try_cast(customer_id as bigint) as customer_id,
    try_cast(membership_tier as varchar) as membership_tier,
    try_cast(created_at as timestamptz) as created_at,
    try_cast(updated_at as timestamptz) as updated_at,
    try_cast(ingested_at as timestamptz) as ingested_at,
    try_cast(dt as date) as ingestion_date
from {{ raw_partition('customers') }}
