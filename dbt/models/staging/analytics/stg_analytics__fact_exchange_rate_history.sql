select
    cast(id as bigint) as exchange_rate_history_id,
    cast(pipeline_run_id as bigint) as pipeline_run_id,
    cast(base_currency as varchar(10)) as base_currency,
    cast(target_currency as varchar(10)) as target_currency,
    cast(rate as numeric(18, 8)) as rate,
    cast(reference_date as date) as reference_date,
    cast(loaded_at as timestamp) as loaded_at
from {{ source('analytics', 'fact_exchange_rate_history') }}
