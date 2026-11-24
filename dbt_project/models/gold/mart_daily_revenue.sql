with order_tiers as (
    select
        cast(o.created_at at time zone 'UTC' as date) as order_date,
        d.membership_tier,
        o.order_status,
        o.amount
    from {{ ref('fct_orders') }} as o
    inner join {{ ref('dim_customers') }} as d
        on o.customer_version_id = d.customer_version_id
)

select
    order_date,
    membership_tier,
    count(*) as total_orders,
    sum(amount) as gmv,
    sum(case when order_status = 'completed' then amount else 0 end)
        as net_revenue
from order_tiers
group by order_date, membership_tier
