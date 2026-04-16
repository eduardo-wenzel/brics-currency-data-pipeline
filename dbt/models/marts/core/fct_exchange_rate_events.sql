select
    exchange_rate_event_id,
    currency,
    rate,
    recorded_at
from {{ ref('stg_analytics__exchange_rates') }}
