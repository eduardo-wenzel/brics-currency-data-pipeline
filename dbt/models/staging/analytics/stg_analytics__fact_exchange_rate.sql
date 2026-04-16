select
    cast(id as bigint) as exchange_rate_id,
    cast(base_currency as varchar(10)) as base_currency,
    cast(target_currency as varchar(10)) as target_currency,
    cast(rate as numeric(18, 8)) as rate,
    cast(reference_date as date) as reference_date,
    cast(created_at as timestamp) as created_at
from {{ source('analytics', 'fact_exchange_rate') }}
