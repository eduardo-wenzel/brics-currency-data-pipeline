from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from pipeline.extract import fetch_exchange_rates, save_raw_data
from pipeline.load import create_pipeline_run, finalize_pipeline_run, load_to_postgres
from pipeline.migrations import apply_migrations
from pipeline.transform import save_processed_data, transform_raw_file


def _should_skip_db_load() -> bool:
    return os.getenv("SKIP_DB_LOAD", "false").strip().lower() in {"1", "true", "yes"}


@dag(
    dag_id="brics_currency_pipeline",
    description="Coleta, transforma e carrega cotacoes BRICS com Airflow.",
    schedule=os.getenv("AIRFLOW_SCHEDULE", "0 */6 * * *"),
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["brics", "fx", "elt"],
)
def brics_currency_pipeline():
    @task
    def extract_task() -> str:
        payload = fetch_exchange_rates()
        return str(save_raw_data(payload))

    @task
    def transform_task(raw_file: str) -> dict[str, str | int]:
        dataframe = transform_raw_file(raw_file)
        processed_file = save_processed_data(dataframe)
        return {
            "processed_file": str(processed_file),
            "records": len(dataframe),
            "raw_file": raw_file,
        }

    @task
    def load_task(transform_result: dict[str, str | int]) -> dict[str, str | int | None]:
        if _should_skip_db_load():
            return {
                "status": "SKIPPED",
                "records_loaded": 0,
                "processed_file": transform_result["processed_file"],
            }

        apply_migrations()
        dataframe = pd.read_parquet(transform_result["processed_file"])
        run_id = create_pipeline_run()

        try:
            loaded_count = load_to_postgres(dataframe, run_id=run_id)
            finalize_pipeline_run(run_id, status="SUCCESS", records_loaded=loaded_count)
            return {
                "status": "SUCCESS",
                "records_loaded": loaded_count,
                "processed_file": transform_result["processed_file"],
                "run_id": run_id,
            }
        except Exception as exc:
            finalize_pipeline_run(run_id, status="FAILED", records_loaded=0, error_message=str(exc))
            raise

    load_task(transform_task(extract_task()))


dag = brics_currency_pipeline()
