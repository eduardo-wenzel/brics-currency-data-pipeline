select
    base_currency,
    target_currency,
    reference_date,
    count(*) as record_count
from {{ ref('fct_exchange_rate_current') }}
group by 1, 2, 3
having count(*) > 1
