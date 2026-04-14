import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _get_connection():
    """Open a PostgreSQL connection using project environment variables."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", 5432),
    )


def create_pipeline_run() -> int:
    """Create a pipeline execution log row and return its run id."""
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO analytics.pipeline_run_log (status)
                VALUES ('RUNNING')
                RETURNING run_id;
                """
            )
            run_id = cursor.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        conn.close()


def finalize_pipeline_run(
    run_id: int, status: str, records_loaded: int = 0, error_message: str | None = None
):
    """Finalize a pipeline execution log row with outcome metadata."""
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE analytics.pipeline_run_log
                SET finished_at = CURRENT_TIMESTAMP,
                    status = %s,
                    records_loaded = %s,
                    error_message = %s
                WHERE run_id = %s;
                """,
                (status, records_loaded, error_message, run_id),
            )
        conn.commit()
    finally:
        conn.close()


def load_to_postgres(df, run_id: int | None = None):
    """Load the silver dataframe into the analytics tables."""
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            snapshot_query = """
                INSERT INTO analytics.fact_exchange_rate
                (base_currency, target_currency, rate, reference_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (base_currency, target_currency, reference_date)
                DO UPDATE SET rate = EXCLUDED.rate;
            """

            history_query = """
                INSERT INTO analytics.fact_exchange_rate_history
                (pipeline_run_id, base_currency, target_currency, rate, reference_date)
                VALUES (%s, %s, %s, %s, %s);
            """

            exchange_rates_query = """
                INSERT INTO analytics.exchange_rates
                (currency, rate, "timestamp")
                VALUES (%s, %s, CURRENT_TIMESTAMP);
            """

            records = [
                (
                    record["base_currency"],
                    record["target_currency"],
                    record["rate"],
                    record["reference_date"],
                )
                for record in df.to_dict("records")
            ]

            execute_batch(cursor, snapshot_query, records)

            history_records = [(run_id, *record) for record in records]
            execute_batch(cursor, history_query, history_records)

            exchange_rate_records = [(record[1], record[2]) for record in records]
            execute_batch(cursor, exchange_rates_query, exchange_rate_records)

        conn.commit()
        return len(records)
    finally:
        conn.close()


def main():
    """Prevent direct execution of the load step without the orchestrated flow."""
    raise SystemExit("Use pipeline/run.py para executar o fluxo completo.")


if __name__ == "__main__":
    main()
