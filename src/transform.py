import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd


def get_latest_raw_file():
    raw_path = Path("data/raw")
    files = list(raw_path.glob("*.json"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo raw encontrado.")

    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logging.info(f"Arquivo raw mais recente: {latest_file}")
    return latest_file


def transform_latest_file():
    file_path = get_latest_raw_file()

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Validação estrutural robusta
    if "conversion_rates" not in data:
        raise ValueError("Estrutura inesperada: campo 'conversion_rates' não encontrado.")

    if "base_code" not in data:
        raise ValueError("Estrutura inesperada: campo 'base_code' não encontrado.")

    if "time_last_update_utc" not in data:
        raise ValueError("Estrutura inesperada: campo 'time_last_update_utc' não encontrado.")

    base_currency = data["base_code"]
    rates = data["conversion_rates"]

    # Converte data oficial da API
    reference_date = datetime.strptime(
        data["time_last_update_utc"],
        "%a, %d %b %Y %H:%M:%S %z"
    ).date()

    records = [
        {
            "base_currency": base_currency,
            "target_currency": currency,
            "rate": rate,
            "reference_date": reference_date
        }
        for currency, rate in rates.items()
    ]

    df = pd.DataFrame(records)

    logging.info(f"Transform gerou {len(df)} registros.")
    logging.info(f"Colunas: {df.columns.tolist()}")

    return df


def save_processed_data(df):
    processed_path = Path("data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = processed_path / f"brics_rates_{timestamp}.parquet"

    df.to_parquet(output_file, index=False)

    logging.info(f"Arquivo parquet salvo em: {output_file}")
    return output_file