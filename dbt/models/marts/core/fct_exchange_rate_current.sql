select
    exchange_rate_id,
    base_currency,
    target_currency,
    rate,
    reference_date,
    created_at
from {{ ref('stg_analytics__fact_exchange_rate') }}
