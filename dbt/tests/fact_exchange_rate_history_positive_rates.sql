select *
from {{ source('analytics', 'fact_exchange_rate_history') }}
where rate <= 0
   or rate is null
