select *
from {{ ref('dim_pipeline_runs') }}
where records_loaded < 0
