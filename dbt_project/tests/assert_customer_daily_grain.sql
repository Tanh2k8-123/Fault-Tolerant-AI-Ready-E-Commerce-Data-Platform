select customer_id, ingestion_date
from {{ ref('silver_customers_daily') }}
group by customer_id, ingestion_date
having count(*) > 1
