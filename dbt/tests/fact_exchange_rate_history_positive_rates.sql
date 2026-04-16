select *
from {{ ref('fct_exchange_rate_history') }}
where rate <= 0
   or rate is null
