import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", 5432),
    )


def _ensure_tables(cursor):
    cursor.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.fact_exchange_rate (
            id BIGSERIAL PRIMARY KEY,
            base_currency VARCHAR(10) NOT NULL,
            target_currency VARCHAR(10) NOT NULL,
            rate NUMERIC(18,8) NOT NULL,
            reference_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_exchange_unique
            ON analytics.fact_exchange_rate (base_currency, target_currency, reference_date);

        CREATE TABLE IF NOT EXISTS analytics.fact_exchange_rate_history (
            id BIGSERIAL PRIMARY KEY,
            pipeline_run_id BIGINT,
            base_currency VARCHAR(10) NOT NULL,
            target_currency VARCHAR(10) NOT NULL,
            rate NUMERIC(18,8) NOT NULL,
            reference_date DATE NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.exchange_rates (
            id BIGSERIAL PRIMARY KEY,
            currency VARCHAR(10) NOT NULL,
            rate NUMERIC(18,8) NOT NULL,
            "timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.pipeline_run_log (
            run_id BIGSERIAL PRIMARY KEY,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            records_loaded INTEGER NOT NULL DEFAULT 0,
            error_message TEXT
        );
        """
    )


def create_pipeline_run() -> int:
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
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


def finalize_pipeline_run(run_id: int, status: str, records_loaded: int = 0, error_message: str | None = None):
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
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
    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)

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
                    row["base_currency"],
                    row["target_currency"],
                    row["rate"],
                    row["reference_date"],
                )
                for _, row in df.iterrows()
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
    raise SystemExit("Use pipeline/run.py para executar o fluxo completo.")


if __name__ == "__main__":
    main()
