select *
from {{ ref('dim_pipeline_runs') }}
where
    (status = 'RUNNING' and finished_at is not null)
    or (status in ('SUCCESS', 'FAILED') and finished_at is null)
