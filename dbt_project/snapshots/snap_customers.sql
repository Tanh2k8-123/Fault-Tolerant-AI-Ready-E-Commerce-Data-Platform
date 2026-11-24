{% snapshot snap_customers %}

{{ config(target_schema=target.schema, unique_key='customer_id',
          strategy='timestamp', updated_at='updated_at') }}

select
    customer_id,
    membership_tier,
    source_row_id,
    updated_at at time zone 'UTC' as updated_at
from {{ ref('stg_bronze_customers') }}
{% if var('partition_date', none) is not none %}
    where ingestion_date <= date '{{ modules.datetime.date.fromisoformat(var("partition_date") | string) }}'
{% endif %}
qualify row_number() over (
    partition by customer_id
    order by updated_at desc, ingested_at desc, source_row_id desc
) = 1

{% endsnapshot %}
