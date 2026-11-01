#!/usr/bin/env bash
# Runs once, on the first start of the local postgres container (docker-entrypoint-initdb.d).
# The Airflow metadata DB always lives here, also on EC2; shop/warehouse are created by sql/init/init.sh.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -v pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow;
SQL
