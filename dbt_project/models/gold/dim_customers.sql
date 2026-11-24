{{ config(materialized='table') }}

-- A first snapshot taken after several arrivals observes only the latest state.
-- Fill missed versions from immutable daily history before closing intervals.
with observed as (
    select
        customer_id,
        membership_tier,
        source_row_id,
        dbt_valid_from at time zone 'UTC' as valid_from,
        dbt_valid_to at time zone 'UTC' as snapshot_end,
        1 as snapshot_priority
    from {{ ref('snap_customers') }}

    union all

    select
        customer_id,
        membership_tier,
        source_row_id,
        updated_at as valid_from,
        cast(null as timestamptz) as snapshot_end,
        0 as snapshot_priority
    from {{ ref('silver_customers_daily') }}
),

versions as (
    select
        customer_id,
        membership_tier,
        valid_from,
        snapshot_end
    from observed
    qualify row_number() over (
        partition by customer_id, valid_from
        order by source_row_id desc, snapshot_priority desc
    ) = 1
),

intervals as (
    select
        customer_id,
        membership_tier,
        valid_from,
        least(
            snapshot_end,
            lead(valid_from) over (
                partition by customer_id order by valid_from
            )
        ) as valid_to
    from versions
)

select
    customer_id,
    membership_tier,
    valid_from,
    valid_to,
    concat(customer_id, ':', epoch_us(valid_from)) as customer_version_id,
    valid_to is null as is_current
from intervals
