import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd


def get_latest_raw_file():
    raw_path = Path("data/raw")
    files = list(raw_path.glob("*.json"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo raw encontrado.")

    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logging.info(f"Arquivo raw mais recente: {latest_file}")
    return latest_file


def _parse_reference_date(raw_value: str):
    try:
        return datetime.strptime(raw_value, "%a, %d %b %Y %H:%M:%S %z").date()
    except ValueError:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()


def transform_latest_file():
    file_path = get_latest_raw_file()

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rates = data.get("conversion_rates") or data.get("rates")
    if not rates:
        raise ValueError("Estrutura inesperada: taxas de cambio nao encontradas.")

    base_currency = data.get("base_code") or data.get("base")
    if not base_currency:
        raise ValueError("Estrutura inesperada: moeda base nao encontrada.")

    raw_reference_date = data.get("time_last_update_utc") or data.get("date")
    if not raw_reference_date:
        raise ValueError("Estrutura inesperada: data de referencia nao encontrada.")

    reference_date = _parse_reference_date(raw_reference_date)

    records = [
        {
            "base_currency": str(base_currency).upper(),
            "target_currency": currency,
            "rate": rate,
            "reference_date": reference_date,
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
