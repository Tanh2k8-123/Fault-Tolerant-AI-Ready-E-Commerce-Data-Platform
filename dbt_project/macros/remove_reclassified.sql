{% macro remove_reclassified(quarantine) %}
    {% if is_incremental() %}
        delete from {{ this }} as previous
        using {{ ref('int_orders_current') }} as latest
        where previous.order_id = latest.order_id
        {% if quarantine %}
            and (latest.error_reason is null or previous.source_row_id <> latest.source_row_id)
        {% else %}
            and latest.error_reason is not null
        {% endif %}
    {% endif %}
{% endmacro %}
