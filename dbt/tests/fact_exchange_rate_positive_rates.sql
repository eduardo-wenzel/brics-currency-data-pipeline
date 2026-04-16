select *
from {{ source('analytics', 'fact_exchange_rate') }}
where rate <= 0
   or rate is null
