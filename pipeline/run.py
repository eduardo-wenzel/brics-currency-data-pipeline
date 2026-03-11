import logging
import os
import time

try:
    from pipeline.extract import fetch_exchange_rates, save_raw_data
    from pipeline.load import create_pipeline_run, finalize_pipeline_run, load_to_postgres
    from pipeline.transform import save_processed_data, transform_latest_file
except ImportError:
    from extract import fetch_exchange_rates, save_raw_data
    from load import create_pipeline_run, finalize_pipeline_run, load_to_postgres
    from transform import save_processed_data, transform_latest_file


def _should_skip_db_load() -> bool:
    return os.getenv("SKIP_DB_LOAD", "false").strip().lower() in {"1", "true", "yes"}


def run_pipeline():
    start = time.time()
    run_id = None

    logging.info("Iniciando pipeline...")

    try:
        data = fetch_exchange_rates()
        logging.info("API request successful")

        raw_file = save_raw_data(data)
        df = transform_latest_file()
        processed_file = save_processed_data(df)
        processed_count = len(df)
        logging.info(f"{processed_count} currencies processed")
        logging.info(f"Raw salvo em: {raw_file}")
        logging.info(f"Silver salvo em: {processed_file}")

        if _should_skip_db_load():
            logging.info("SKIP_DB_LOAD=true: carga no PostgreSQL ignorada.")
        else:
            run_id = create_pipeline_run()
            loaded_count = load_to_postgres(df, run_id=run_id)
            finalize_pipeline_run(run_id, status="SUCCESS", records_loaded=loaded_count)
            logging.info("Data loaded into database")

        duration = time.time() - start
        logging.info(f"Pipeline finalizado em {duration:.2f} segundos.")

    except Exception as exc:
        if run_id is not None:
            try:
                finalize_pipeline_run(
                    run_id, status="FAILED", records_loaded=0, error_message=str(exc)
                )
            except Exception:
                logging.exception("Falha ao atualizar pipeline_run_log.")

        logging.exception("Falha na execucao do pipeline.")
        raise


if __name__ == "__main__":
    run_pipeline()
