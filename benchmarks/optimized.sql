with filtered as (
    select order_date, customer_version_id, amount
    from benchmark_orders
    where
        order_status = 'completed'
        and order_date >= getvariable('report_month') - 29
        and order_date < cast(getvariable('report_month') + interval '1 month' as date)
),

daily as (
    select
        o.order_date,
        d.membership_tier,
        sum(o.amount) as net_revenue
    from filtered as o
    inner join dim_customers as d
        on o.customer_version_id = d.customer_version_id
    group by order_date, d.membership_tier
),

calendar as (
    select cast(day as date) as order_date
    from generate_series(
        getvariable('report_month') - interval '29 days',
        getvariable('report_month') + interval '1 month' - interval '1 day',
        interval '1 day'
    ) as dates(day)
),

tiers as (
    select distinct membership_tier from dim_customers
),

rolling as (
    select
        c.order_date,
        t.membership_tier,
        sum(coalesce(d.net_revenue, 0)) over (
            partition by t.membership_tier order by c.order_date
            rows between 29 preceding and current row
        ) as net_revenue_30d
    from calendar as c
    cross join tiers as t
    left join daily as d
        on c.order_date = d.order_date and t.membership_tier = d.membership_tier
)

select order_date, membership_tier, net_revenue_30d
from rolling
where order_date >= getvariable('report_month')
order by order_date, membership_tier
