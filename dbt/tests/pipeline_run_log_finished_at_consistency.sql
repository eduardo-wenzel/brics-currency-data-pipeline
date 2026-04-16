select *
from {{ source('analytics', 'pipeline_run_log') }}
where
    (status = 'RUNNING' and finished_at is not null)
    or (status in ('SUCCESS', 'FAILED') and finished_at is null)
