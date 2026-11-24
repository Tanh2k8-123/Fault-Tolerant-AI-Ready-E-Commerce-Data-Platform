select
    product_id,
    product_name,
    category,
    list_price,
    updated_at
from {{ ref('silver_products') }}
