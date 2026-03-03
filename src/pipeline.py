from ingest import fetch_exchange_rates, save_raw_data
from transform import transform_latest_file
import logging
import time
from pathlib import Path
import datetime
from load import load_to_postgres

def run_pipeline():
    start = time.time()
    
    logging.info("Iniciando pipeline...")

    try:
        data = fetch_exchange_rates()
        raw_file = save_raw_data(data)
        df = transform_latest_file()
        duration  = time.time() - start
        logging.info(f"Raw salvo em: {raw_file}")

        logging.info(f"Pipeline finalizado em {duration:.2f} segundos.")
        load_to_postgres(df)
        logging.info("Dados carregados no PostgreSQL.")


    except Exception:
        logging.exception("Falha na execução do pipeline.")
        raise


def save_processed_data(df):
    processed_path = Path("data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = processed_path / f"brics_rates_{timestamp}.parquet"

    df.to_parquet(output_file, index=False)

    logging.info(f"Arquivo parquet salvo em: {output_file}")
    return output_file

if __name__ == "__main__":
    run_pipeline()