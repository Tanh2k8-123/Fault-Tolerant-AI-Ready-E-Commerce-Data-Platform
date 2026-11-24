with ordered as (
    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id order by valid_from
        ) as next_from
    from {{ ref('dim_customers') }}
)

select customer_id, valid_from, valid_to, next_from
from ordered
where
    valid_to <= valid_from
    or (next_from is not null and (valid_to is null or valid_to > next_from))
