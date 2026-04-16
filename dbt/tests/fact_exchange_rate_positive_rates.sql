select *
from {{ ref('fct_exchange_rate_current') }}
where rate <= 0
   or rate is null
