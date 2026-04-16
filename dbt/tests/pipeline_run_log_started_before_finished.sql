select *
from {{ source('analytics', 'pipeline_run_log') }}
where finished_at is not null
  and finished_at < started_at
