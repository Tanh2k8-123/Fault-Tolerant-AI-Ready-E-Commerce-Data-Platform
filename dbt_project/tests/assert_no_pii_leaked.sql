select review_id
from {{ ref('silver_reviews') }}
where
    regexp_matches(review_text, '[[:alnum:]._%+-]+@[[:alnum:].-]+\.[A-Za-z]{2,}')
    or regexp_matches(review_text, '\+?1[- .]?\(?[0-9]{3}\)?[- .]?[0-9]{3}[- .]?[0-9]{4}\b')
