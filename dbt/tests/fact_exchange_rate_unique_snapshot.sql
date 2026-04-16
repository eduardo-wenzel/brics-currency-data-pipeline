select
    base_currency,
    target_currency,
    reference_date,
    count(*) as record_count
from {{ source('analytics', 'fact_exchange_rate') }}
group by 1, 2, 3
having count(*) > 1
