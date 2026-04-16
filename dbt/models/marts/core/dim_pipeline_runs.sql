select
    pipeline_run_id,
    started_at,
    finished_at,
    status,
    records_loaded,
    error_message
from {{ ref('stg_analytics__pipeline_run_log') }}
