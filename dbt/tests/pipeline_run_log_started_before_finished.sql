select *
from {{ ref('dim_pipeline_runs') }}
where finished_at is not null
  and finished_at < started_at
