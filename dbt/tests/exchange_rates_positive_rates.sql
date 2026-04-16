select *
from {{ source('analytics', 'exchange_rates') }}
where rate <= 0
   or rate is null
