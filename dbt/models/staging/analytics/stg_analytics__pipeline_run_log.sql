select
    cast(run_id as bigint) as pipeline_run_id,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    cast(status as varchar(20)) as status,
    cast(records_loaded as integer) as records_loaded,
    cast(error_message as text) as error_message
from {{ source('analytics', 'pipeline_run_log') }}
