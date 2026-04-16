with history_counts as (
    select
        pipeline_run_id,
        count(*) as history_records
    from {{ source('analytics', 'fact_exchange_rate_history') }}
    where pipeline_run_id is not null
    group by 1
)
select
    log.run_id,
    log.records_loaded,
    coalesce(history.history_records, 0) as history_records
from {{ source('analytics', 'pipeline_run_log') }} as log
left join history_counts as history
    on log.run_id = history.pipeline_run_id
where log.status = 'SUCCESS'
  and log.records_loaded <> coalesce(history.history_records, 0)
