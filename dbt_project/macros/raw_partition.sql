{% macro raw_partition(table_name) %}
    {% set batch = var('batch_id', 'demo01') %}
    {% if not modules.re.fullmatch('[A-Za-z0-9_-]+', batch | string) %}
        {{ exceptions.raise_compiler_error('Invalid batch_id') }}
    {% endif %}
    {% if target.name == 'minio' %}
        {% set root = 's3://' ~ env_var('MINIO_BUCKET', 'ecommerce') ~ '/raw' %}
    {% else %}
        {% set root = env_var('RAW_DIR', 'data/raw').replace('\\', '/').rstrip('/') %}
    {% endif %}
    read_parquet(
        '{{ root | replace("'", "''") }}/{{ table_name }}/dt=*/part-{{ batch }}-[0-9][0-9][0-9][0-9][0-9].parquet',
        hive_partitioning = true
    )
{% endmacro %}

{% macro ingestion_window() %}
    {% set day = var('partition_date', none) %}
    {% if day is not none %}
        {% set cutoff = modules.datetime.date.fromisoformat(day | string).isoformat() %}
        where ingestion_date <= date '{{ cutoff }}'
        {% if is_incremental() %}
            and ingestion_date >= date '{{ cutoff }}' - 3
        {% endif %}
    {% elif is_incremental() %}
        where ingestion_date >= coalesce(
            (select max(ingestion_date) from {{ this }}), date '0001-01-01'
        ) - 3
    {% endif %}
{% endmacro %}
