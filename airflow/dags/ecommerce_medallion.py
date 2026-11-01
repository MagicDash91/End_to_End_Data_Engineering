"""Daily e-commerce medallion pipeline.

simulate_source_activity -> ingest_bronze -> bronze_to_silver -> dbt_source_freshness -> dbt_build_gold -> report_gold

* simulate_source_activity: seeds the shop DB on first run, then adds a batch of new orders (demo only;
  set the `simulate_orders` param to 0 to ingest just what is already there).
* ingest_bronze: Airbyte sync of shop -> warehouse.bronze (INGESTION_MODE=airbyte), or the python loader
  that writes the same table shape (INGESTION_MODE=python).
* bronze_to_silver: Spark job on the standalone cluster, cleans/validates/de-duplicates into silver.
* dbt: source freshness on silver, then `dbt build` (models + tests) for gold.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_ARGS = (f"--project-dir {PROJECT_DIR}/dbt --profiles-dir {PROJECT_DIR}/dbt "
            "--target-path /tmp/dbt/target --log-path /tmp/dbt/logs")
log = logging.getLogger(__name__)


@dag(
    dag_id="ecommerce_medallion",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"simulate_orders": Param(150, type="integer", minimum=0,
                                     description="New source orders to generate before ingesting (0 = none)")},
    tags=["medallion", "airbyte", "spark", "dbt"],
)
def ecommerce_medallion():

    @task
    def simulate_source_activity() -> dict:
        from pipeline.db import connect, shop_db
        from pipeline.generator import seed, simulate

        n_orders = get_current_context()["params"]["simulate_orders"]
        with connect(shop_db()) as conn:
            seeded = seed(conn)
            if "skipped" not in seeded:
                log.info("seeded empty shop: %s", seeded)
                return seeded
            result = simulate(conn, n_orders=n_orders) if n_orders else {"new_orders": 0}
        log.info("source activity: %s", result)
        return result

    @task
    def ingest_bronze() -> dict:
        mode = os.environ.get("INGESTION_MODE", "python")
        if mode == "python":
            from pipeline.bronze_loader import load_all

            loaded = load_all()
            log.info("python loader appended to bronze: %s", loaded)
            return {"mode": mode, "rows": loaded}
        if mode == "airbyte":
            from pipeline.airbyte_client import AirbyteClient

            client = AirbyteClient()
            connection_id = client.connection_id(os.environ.get("AIRBYTE_CONNECTION_NAME", "shop-to-bronze"))
            job_id = client.trigger_sync(connection_id)
            job = client.wait_for_job(job_id, log=log.info)
            return {"mode": mode, "job_id": job_id, "rows": job.get("rowsSynced")}
        raise ValueError(f"unknown INGESTION_MODE {mode!r} (expected 'airbyte' or 'python')")

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_default",
        application=f"{PROJECT_DIR}/spark/jobs/bronze_to_silver.py",
        py_files=f"{PROJECT_DIR}/spark/jobs/transforms.py",
        jars=os.environ.get("SPARK_JDBC_JAR", "/opt/spark-jars/postgresql.jar"),
        name="ecommerce-bronze-to-silver",
        conf={
            # Executors connect back to the driver, which runs inside this Airflow container.
            "spark.driver.host": os.environ.get("SPARK_DRIVER_HOST", "airflow"),
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"),
            "spark.pyspark.python": "python3",
            "spark.sql.session.timeZone": "UTC",
        },
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_BIN} source freshness {DBT_ARGS}",
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"{DBT_BIN} build {DBT_ARGS}",
    )

    @task
    def report_gold() -> dict:
        from pipeline.db import connect, warehouse_db

        with connect(warehouse_db()) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT coalesce(sum(revenue), 0), coalesce(sum(revenue_orders), 0)
                FROM gold.mart_daily_revenue WHERE date_day > current_date - 7
            """)
            revenue_7d, orders_7d = cur.fetchone()
            cur.execute("SELECT entity, reason, rejected_rows FROM gold.mart_data_quality ORDER BY rejected_rows DESC")
            quality = [{"entity": e, "reason": r, "rows": n} for e, r, n in cur.fetchall()]
        summary = {"revenue_7d_idr": float(revenue_7d), "revenue_orders_7d": int(orders_7d), "quarantine": quality}
        log.info("gold summary: %s", summary)
        return summary

    (simulate_source_activity() >> ingest_bronze() >> bronze_to_silver
     >> dbt_source_freshness >> dbt_build_gold >> report_gold())


ecommerce_medallion()
