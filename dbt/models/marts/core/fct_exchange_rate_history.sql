select
    exchange_rate_history_id,
    pipeline_run_id,
    base_currency,
    target_currency,
    rate,
    reference_date,
    loaded_at
from {{ ref('stg_analytics__fact_exchange_rate_history') }}
