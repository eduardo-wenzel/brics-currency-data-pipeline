import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests import RequestException

from pipeline.storage import save_raw_data as persist_raw_data

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

log_dir = Path("logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_dir / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

BASE_URL = os.getenv("API_URL")
FALLBACK_URL = os.getenv("API_FALLBACK_URL", "").strip()


def _api_candidates() -> list[str]:
    """Return configured API endpoints in priority order."""
    return [url for url in [BASE_URL, FALLBACK_URL] if url]


def _max_retries() -> int:
    """Return the number of attempts per API endpoint."""
    return max(1, int(os.getenv("API_MAX_RETRIES", "3")))


def _retry_backoff_seconds() -> float:
    """Return the initial retry backoff in seconds."""
    return max(0.0, float(os.getenv("API_RETRY_BACKOFF_SECONDS", "1.0")))


def _fetch_from_url(url: str) -> dict:
    """Fetch exchange-rate data from a single endpoint."""
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()

    if data.get("result") == "error":
        raise RuntimeError(f"Falha na API: {data.get('error-type', 'erro desconhecido')}")

    return data


def fetch_exchange_rates():
    """Fetch exchange-rate payload with retry and optional fallback endpoint."""
    candidates = _api_candidates()
    if not candidates:
        raise OSError("Variavel de ambiente API_URL nao configurada.")

    start = time.time()
    last_error = None

    for url in candidates:
        for attempt in range(1, _max_retries() + 1):
            try:
                data = _fetch_from_url(url)
                duration = time.time() - start
                logging.info(f"Payload bruto recebido da API em {duration:.2f} segundos.")
                logging.info(f"Fonte utilizada: {url}")
                return data
            except (RequestException, RuntimeError, ValueError) as exc:
                last_error = exc
                is_last_attempt = attempt == _max_retries()
                logging.warning(
                    "Falha ao consultar API %s na tentativa %s/%s: %s",
                    url,
                    attempt,
                    _max_retries(),
                    exc,
                )
                if not is_last_attempt:
                    time.sleep(_retry_backoff_seconds() * (2 ** (attempt - 1)))

        logging.warning("Endpoint %s esgotado. Tentando proxima fonte, se houver.", url)

    raise RuntimeError("Falha ao obter cotacoes da API primaria e da fallback.") from last_error


def save_raw_data(data: dict):
    """Persist the raw payload in the configured bronze layer."""
    file_path = persist_raw_data(data)
    logging.info(f"Arquivo raw salvo em: {file_path}")
    return file_path


def main():
    """Run the extract step as a module entrypoint."""
    data = fetch_exchange_rates()
    save_raw_data(data)
    logging.info("Ingest executado com sucesso.")


if __name__ == "__main__":
    main()
