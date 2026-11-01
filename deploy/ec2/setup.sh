#!/bin/bash
# Runs on the EC2 host as root, from the release just unpacked into /opt/de-project/current
# (called by /usr/local/bin/de-deploy, which the Terraform user_data installs).
#  1. builds .env from the RDS secret (shared across releases, never in the bundle)
#  2. installs/starts Airbyte with abctl when INGESTION_MODE=airbyte
#  3. docker compose up (Postgres for Airflow metadata, db-init against RDS, Spark, Airflow)
#  4. creates the Airbyte source/destination/connection if they don't exist yet
set -euo pipefail
source /etc/de-project.env

APP=/opt/de-project/current
SHARED=/opt/de-project/shared
mkdir -p "$SHARED"
cd "$APP"

secret=$(aws secretsmanager get-secret-value --secret-id "$DB_SECRET_ARN" --query SecretString --output text)
pg_host=$(jq -r .host <<<"$secret")
pg_port=$(jq -r .port <<<"$secret")
pg_user=$(jq -r .username <<<"$secret")
pg_password=$(jq -r .password <<<"$secret")

if [ ! -s "$SHARED/airflow_db_password" ]; then
  (umask 077 && openssl rand -hex 24 > "$SHARED/airflow_db_password")
fi

airbyte_client_id=""
airbyte_client_secret=""
if [ "$INGESTION_MODE" = "airbyte" ]; then
  export PATH="/usr/local/bin:$PATH"
  if ! command -v abctl >/dev/null; then
    curl -LsfS https://get.airbyte.com | bash -
  fi
  if ! abctl local status >/dev/null 2>&1; then
    # First install pulls several GB of images and takes 10-20 minutes.
    abctl local install --port 8000
  fi
  creds=$(abctl local credentials 2>&1)
  airbyte_client_id=$(awk '/Client-Id:/ {print $NF}' <<<"$creds")
  airbyte_client_secret=$(awk '/Client-Secret:/ {print $NF}' <<<"$creds")
fi

(umask 077 && cat > "$SHARED/.env" <<EOF
PG_HOST=$pg_host
PG_PORT=$pg_port
PG_USER=$pg_user
PG_PASSWORD=$pg_password
PG_SSLMODE=require
SHOP_DB=shop
WAREHOUSE_DB=warehouse
AIRFLOW_DB_PASSWORD=$(cat "$SHARED/airflow_db_password")
INGESTION_MODE=$INGESTION_MODE
AIRBYTE_API_URL=http://host.docker.internal:8000/api/public/v1
AIRBYTE_CLIENT_ID=$airbyte_client_id
AIRBYTE_CLIENT_SECRET=$airbyte_client_secret
AIRBYTE_CONNECTION_NAME=shop-to-bronze
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=3g
EOF
)
ln -sfn "$SHARED/.env" "$APP/.env"

docker compose up -d --build --remove-orphans

if [ "$INGESTION_MODE" = "airbyte" ]; then
  # Airbyte connects to RDS directly (not through the compose network).
  docker compose run --rm --no-deps \
    -e AIRBYTE_PG_HOST="$pg_host" -e AIRBYTE_PG_PORT="$pg_port" \
    airflow python /opt/project/scripts/airbyte_bootstrap.py
fi

docker compose ps
echo "deploy finished: $(readlink -f "$APP")"
