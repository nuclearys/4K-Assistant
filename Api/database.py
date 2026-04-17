from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from Api.config import settings

DEFAULT_LEVEL_PERCENT_MAP = {
    "L1": 45,
    "L2": 70,
    "L3": 92,
    "N/A": 0,
}


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        row_factory=dict_row,
    )


def ensure_core_schema() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS company_industry TEXT
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS assessment_level_weights (
                level_code TEXT PRIMARY KEY,
                percent_value INTEGER NOT NULL CHECK (percent_value >= 0 AND percent_value <= 100)
            )
            """
        )
        for level_code, percent_value in DEFAULT_LEVEL_PERCENT_MAP.items():
            connection.execute(
                """
                INSERT INTO assessment_level_weights (level_code, percent_value)
                VALUES (%s, %s)
                ON CONFLICT (level_code) DO NOTHING
                """,
                (level_code, percent_value),
            )
        connection.commit()


def get_level_percent_map(connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT level_code, percent_value
        FROM assessment_level_weights
        """
    ).fetchall()
    level_map = dict(DEFAULT_LEVEL_PERCENT_MAP)
    for row in rows:
        level_map[str(row["level_code"])] = int(row["percent_value"])
    return level_map
