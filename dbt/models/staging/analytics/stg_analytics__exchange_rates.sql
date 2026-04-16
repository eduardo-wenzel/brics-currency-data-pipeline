select
    cast(id as bigint) as exchange_rate_event_id,
    cast(currency as varchar(10)) as currency,
    cast(rate as numeric(18, 8)) as rate,
    cast("timestamp" as timestamp) as recorded_at
from {{ source('analytics', 'exchange_rates') }}
