from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from .schema import SCHEMA_SQL
from .utils import safe_mkdir


class SQLiteDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        safe_mkdir(self.db_path.parent)
        self._ensure_connection()

    def _ensure_connection(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA foreign_keys = ON;")

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def create_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()

    def create_schema_sql(self, schema_sql: str) -> None:
        with self.connect() as conn:
            conn.executescript(schema_sql)
            conn.commit()

    def upsert_dataframe(self, table: str, df: pd.DataFrame, pk_cols: Iterable[str]) -> int:
        if df is None or df.empty:
            return 0
        pk_cols = list(pk_cols)
        columns = list(df.columns)
        update_cols = [column for column in columns if column not in pk_cols]
        column_sql = ", ".join(columns)
        staging_table = f"_stg_{table}_{uuid.uuid4().hex[:8]}"
        clean_df = df.where(pd.notna(df), None)
        with self.connect() as conn:
            clean_df.to_sql(staging_table, conn, if_exists="replace", index=False)
            if pk_cols:
                where_pk = " AND ".join(f"t.{col}=s.{col}" for col in pk_cols)
                if update_cols:
                    set_sql = ", ".join(
                        f"{col}=(SELECT s.{col} FROM {staging_table} AS s WHERE {where_pk})" for col in update_cols
                    )
                    conn.execute(
                        f"UPDATE {table} AS t SET {set_sql} "
                        f"WHERE EXISTS (SELECT 1 FROM {staging_table} AS s WHERE {where_pk})"
                    )
                conn.execute(
                    f"INSERT INTO {table} ({column_sql}) "
                    f"SELECT {column_sql} FROM {staging_table} AS s "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WHERE {where_pk})"
                )
            else:
                conn.execute(f"INSERT INTO {table} ({column_sql}) SELECT {column_sql} FROM {staging_table}")
            conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
            conn.commit()
        return len(clean_df)

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        clean_df = df.where(pd.notna(df), None)
        with self.connect() as conn:
            clean_df.to_sql(table, conn, if_exists="append", index=False)
            conn.commit()
        return len(clean_df)

    def ensure_source(self, source: Mapping[str, str]) -> None:
        frame = pd.DataFrame([source])
        self.upsert_dataframe("sources", frame, ["source_id"])

    def log_raw_import(self, row: Mapping[str, str]) -> None:
        frame = pd.DataFrame([row])
        self.upsert_dataframe("raw_imports", frame, ["import_id"])

    def read_table(self, table: str) -> pd.DataFrame:
        with self.connect() as conn:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)

    def table_row_counts(self) -> dict[str, int]:
        tables = [
            "countries",
            "sports",
            "disciplines",
            "competitions",
            "events",
            "participants",
            "results",
            "sources",
            "raw_imports",
            "sport_federations",
        ]
        counts: dict[str, int] = {}
        with self.connect() as conn:
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = int(cursor.fetchone()[0])
        return counts
