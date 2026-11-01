"""Python fallback for bronze ingestion (INGESTION_MODE=python).

Writes the same table shape Airbyte's Postgres destination produces with "Incremental | Append":
the source columns plus `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta` and
`_airbyte_generation_id`. Rows are appended, never updated, so bronze keeps every version of a row and
silver does the de-duplication -- exactly as with Airbyte. Use one mode per warehouse; don't mix them on
the same bronze tables.
"""
from __future__ import annotations

import json
import uuid

from psycopg2 import sql
from psycopg2.extras import execute_values

from pipeline.db import connect, shop_db, warehouse_db

TABLES = ["customers", "products", "orders", "order_items", "payments"]
CURSOR_COLUMN = "updated_at"
BATCH_SIZE = 5000
AIRBYTE_COLUMNS = [
    ("_airbyte_raw_id", "VARCHAR"),
    ("_airbyte_extracted_at", "TIMESTAMPTZ"),
    ("_airbyte_meta", "JSONB"),
    ("_airbyte_generation_id", "BIGINT"),
]


def _source_columns(shop_cur, table: str) -> list[tuple[str, str]]:
    shop_cur.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass('public.' || %s) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (table,),
    )
    columns = shop_cur.fetchall()
    if not columns:
        raise RuntimeError(f"source table public.{table} not found in {shop_db()}")
    return columns


def _ensure_bronze_table(wh_cur, table: str, columns: list[tuple[str, str]]) -> None:
    column_defs = [
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(type_))
        for name, type_ in columns + AIRBYTE_COLUMNS
    ]
    wh_cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS bronze.{} ({})").format(
            sql.Identifier(table), sql.SQL(", ").join(column_defs))
    )


def _watermark(wh_cur, table: str):
    wh_cur.execute(sql.SQL("SELECT MAX({}) FROM bronze.{}").format(
        sql.Identifier(CURSOR_COLUMN), sql.Identifier(table)))
    return wh_cur.fetchone()[0]


def load_table(shop_conn, wh_conn, table: str) -> int:
    with shop_conn.cursor() as shop_cur, wh_conn.cursor() as wh_cur:
        columns = _source_columns(shop_cur, table)
        _ensure_bronze_table(wh_cur, table, columns)
        watermark = _watermark(wh_cur, table)

    names = [name for name, _ in columns]
    select = sql.SQL("SELECT {} FROM public.{}").format(
        sql.SQL(", ").join(map(sql.Identifier, names)), sql.Identifier(table))
    params: tuple = ()
    if watermark is not None:
        select += sql.SQL(" WHERE {} > %s").format(sql.Identifier(CURSOR_COLUMN))
        params = (watermark,)
    select += sql.SQL(" ORDER BY {}").format(sql.Identifier(CURSOR_COLUMN))

    insert = sql.SQL("INSERT INTO bronze.{} ({}) VALUES %s").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, names + [c for c, _ in AIRBYTE_COLUMNS])))
    meta = json.dumps({"changes": [], "sync_id": None})
    template = "(" + ", ".join(["%s"] * len(names)) + ", %s, now(), %s::jsonb, 0)"

    loaded = 0
    with shop_conn.cursor(name=f"bronze_{table}") as src, wh_conn.cursor() as dst:
        src.execute(select, params)
        while rows := src.fetchmany(BATCH_SIZE):
            execute_values(dst, insert.as_string(dst), [(*row, str(uuid.uuid4()), meta) for row in rows],
                           template=template, page_size=1000)
            loaded += len(rows)
    wh_conn.commit()
    shop_conn.commit()
    return loaded


def load_all(tables: list[str] | None = None) -> dict[str, int]:
    with connect(shop_db()) as shop_conn, connect(warehouse_db()) as wh_conn:
        return {table: load_table(shop_conn, wh_conn, table) for table in tables or TABLES}


if __name__ == "__main__":
    print(load_all())
