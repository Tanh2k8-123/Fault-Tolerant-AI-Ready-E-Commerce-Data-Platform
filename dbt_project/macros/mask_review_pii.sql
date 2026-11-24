{% macro mask_review_pii(column) %}
    regexp_replace(
        regexp_replace(
            {{ column }},
            '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',
            '[REDACTED_EMAIL]', 'g'
        ),
        '\+?1[- .]?\(?[0-9]{3}\)?[- .]?[0-9]{3}[- .]?[0-9]{4}\b',
        '[REDACTED_PHONE]', 'g'
    )
{% endmacro %}
