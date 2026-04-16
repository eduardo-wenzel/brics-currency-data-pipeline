select *
from {{ ref('fct_exchange_rate_events') }}
where rate <= 0
   or rate is null
