import psycopg2
import os
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

def load_to_postgres(df):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", 5432)
    )

    cursor = conn.cursor()

    query = """
        INSERT INTO analytics.fact_exchange_rate 
        (base_currency, target_currency, rate, reference_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (base_currency, target_currency, reference_date)
        DO UPDATE SET rate = EXCLUDED.rate;
    """

    records = [
        (
            row["base_currency"],
            row["target_currency"],
            row["rate"],
            row["reference_date"]
        )
        for _, row in df.iterrows()
    ]

    execute_batch(cursor, query, records)

    conn.commit()
    cursor.close()
    conn.close()