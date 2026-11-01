"""Postgres connections built from the same env vars docker compose and EC2 use (see .env.example)."""
from __future__ import annotations

import os

import psycopg2


def conn_params(dbname: str) -> dict:
    return {
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", "5433")),
        "user": os.environ.get("PG_USER", "de_admin"),
        "password": os.environ.get("PG_PASSWORD", ""),
        "sslmode": os.environ.get("PG_SSLMODE", "disable"),
        "dbname": dbname,
    }


def shop_db() -> str:
    return os.environ.get("SHOP_DB", "shop")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "warehouse")


def connect(dbname: str):
    return psycopg2.connect(**conn_params(dbname))
