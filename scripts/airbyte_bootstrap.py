"""Create (or reuse) the Airbyte source, destination and connection for shop -> warehouse.bronze.

Idempotent: objects are looked up by name first. Run after `abctl local install`:

    abctl local credentials            # prints client-id / client-secret
    AIRBYTE_CLIENT_ID=... AIRBYTE_CLIENT_SECRET=... python scripts/airbyte_bootstrap.py

Airbyte runs in its own kind cluster, so it reaches Postgres through AIRBYTE_PG_HOST/AIRBYTE_PG_PORT
(host.docker.internal:5433 locally, the RDS endpoint on AWS) rather than the compose service name.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pipeline.airbyte_client import AirbyteClient  # noqa: E402

SOURCE_NAME = "shop-postgres"
DESTINATION_NAME = "warehouse-bronze"
STREAMS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}


def _db_config(database: str) -> dict:
    return {
        "host": os.environ.get("AIRBYTE_PG_HOST", "host.docker.internal"),
        "port": int(os.environ.get("AIRBYTE_PG_PORT", "5433")),
        "database": database,
        "username": os.environ.get("PG_USER", "de_admin"),
        "password": os.environ["PG_PASSWORD"],
        "ssl_mode": {"mode": os.environ.get("PG_SSLMODE", "disable")},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }


def source_config() -> dict:
    return {
        "sourceType": "postgres",
        **_db_config(os.environ.get("SHOP_DB", "shop")),
        "schemas": ["public"],
        "replication_method": {"method": "Standard"},
    }


def destination_config() -> dict:
    return {
        "destinationType": "postgres",
        **_db_config(os.environ.get("WAREHOUSE_DB", "warehouse")),
        "schema": "bronze",
    }


def connection_streams() -> list[dict]:
    # Incremental | Append: bronze keeps every version of a row; silver de-duplicates.
    return [
        {"name": name, "syncMode": "incremental_append", "cursorField": ["updated_at"], "primaryKey": [[pk]]}
        for name, pk in STREAMS.items()
    ]


def ensure(client: AirbyteClient, kind: str, name: str, workspace_id: str, body: dict, id_field: str) -> str:
    existing = client.find_by_name(kind, name, workspace_id)
    if existing:
        print(f"{kind[:-1]} {name!r} exists: {existing[id_field]}")
        return existing[id_field]
    created = client.request("POST", f"/{kind}", json={"name": name, "workspaceId": workspace_id, **body})
    print(f"created {kind[:-1]} {name!r}: {created[id_field]}")
    return created[id_field]


def main() -> None:
    # Creating a source/destination/connection runs a check or schema discovery pod; that can take minutes.
    client = AirbyteClient(api_url=os.environ.get("AIRBYTE_API_URL", "http://localhost:8000/api/public/v1"),
                           timeout=300)
    workspace_id = client.default_workspace_id()
    source_id = ensure(client, "sources", SOURCE_NAME, workspace_id,
                       {"configuration": source_config()}, "sourceId")
    destination_id = ensure(client, "destinations", DESTINATION_NAME, workspace_id,
                            {"configuration": destination_config()}, "destinationId")

    name = os.environ.get("AIRBYTE_CONNECTION_NAME", "shop-to-bronze")
    existing = client.find_by_name("connections", name, workspace_id)
    if existing:
        print(f"connection {name!r} exists: {existing['connectionId']}")
        return
    created = client.request("POST", "/connections", json={
        "name": name,
        "sourceId": source_id,
        "destinationId": destination_id,
        "namespaceDefinition": "destination",   # -> the destination's default schema, bronze
        "schedule": {"scheduleType": "manual"},  # Airflow triggers it
        "configurations": {"streams": connection_streams()},
    })
    print(f"created connection {name!r}: {created['connectionId']}")


if __name__ == "__main__":
    main()
