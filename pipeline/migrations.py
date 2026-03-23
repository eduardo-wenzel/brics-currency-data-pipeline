from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_MIGRATIONS_APPLIED = False
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "sql"


def _get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", 5432),
    )


def _migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def apply_migrations(force: bool = False):
    global _MIGRATIONS_APPLIED

    if _MIGRATIONS_APPLIED and not force:
        return

    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS public.schema_migrations (
                    version VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            cursor.execute("SELECT version FROM public.schema_migrations;")
            applied_versions = {row[0] for row in cursor.fetchall()}

            for migration_file in _migration_files():
                version = migration_file.name
                if version in applied_versions:
                    continue

                cursor.execute(migration_file.read_text(encoding="utf-8"))
                cursor.execute(
                    """
                    INSERT INTO public.schema_migrations (version)
                    VALUES (%s);
                    """,
                    (version,),
                )

        conn.commit()
        _MIGRATIONS_APPLIED = True
    finally:
        conn.close()

