-- Deliberately materialize the wide historical join for the baseline.
with joined as materialized (
    select *
    from benchmark_orders
    inner join dim_customers using (customer_version_id)
),

daily as (
    select
        cast(created_at as date) as order_date,
        membership_tier,
        sum(amount) as net_revenue
    from joined
    where order_status = 'completed'
    group by cast(created_at as date), membership_tier
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
where
    year(order_date) = year(getvariable('report_month'))
    and month(order_date) = month(getvariable('report_month'))
order by order_date, membership_tier
