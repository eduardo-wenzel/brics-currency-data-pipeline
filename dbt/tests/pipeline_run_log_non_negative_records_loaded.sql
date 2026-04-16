select *
from {{ source('analytics', 'pipeline_run_log') }}
where records_loaded < 0
