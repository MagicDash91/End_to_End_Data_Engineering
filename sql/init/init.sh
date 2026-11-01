#!/usr/bin/env sh
# Creates the shop + warehouse databases, the medallion schemas and the source tables.
# Idempotent. Used by the `db-init` compose service locally and on EC2 (pointing at RDS).
set -eu

export PGHOST="$PG_HOST" PGPORT="$PG_PORT" PGUSER="$PG_USER" PGPASSWORD="$PG_PASSWORD"
export PGSSLMODE="${PG_SSLMODE:-disable}"
DIR="$(cd "$(dirname "$0")" && pwd)"

until pg_isready -q -d postgres; do
  echo "waiting for postgres at $PGHOST:$PGPORT ..."
  sleep 2
done

psql -v ON_ERROR_STOP=1 -d postgres -v shop_db="$SHOP_DB" -v warehouse_db="$WAREHOUSE_DB" -f "$DIR/01_databases.sql"
psql -v ON_ERROR_STOP=1 -d "$WAREHOUSE_DB" -f "$DIR/02_warehouse_schemas.sql"
psql -v ON_ERROR_STOP=1 -d "$SHOP_DB" -f "$DIR/03_shop_schema.sql"
echo "database init complete"
