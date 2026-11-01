"""Spark job: bronze -> silver.

Reads every bronze table from the warehouse over JDBC, applies the rules in transforms.py and replaces
the silver tables (overwrite with truncate, so the table definitions and grants survive). Rejected rows
from all entities land in silver.quarantine with the rule they broke.

Run by Airflow (SparkSubmitOperator) against the standalone cluster, or by hand:
    spark-submit --master local[*] --jars postgresql.jar --py-files spark/jobs/transforms.py \
        spark/jobs/bronze_to_silver.py
Connection settings come from the PG_* / WAREHOUSE_DB env vars (see .env.example).
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import transforms  # noqa: E402


def jdbc_options() -> dict[str, str]:
    host, port = os.environ.get("PG_HOST", "localhost"), os.environ.get("PG_PORT", "5432")
    database, sslmode = os.environ.get("WAREHOUSE_DB", "warehouse"), os.environ.get("PG_SSLMODE", "disable")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}?sslmode={sslmode}",
        "user": os.environ.get("PG_USER", "de_admin"),
        "password": os.environ.get("PG_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("jdbc").options(**jdbc_options()).option("dbtable", f"bronze.{table}").load()


def write_silver(df: DataFrame, table: str) -> int:
    df = df.withColumn("_silver_loaded_at", F.current_timestamp()).cache()
    count = df.count()
    (df.write.format("jdbc").options(**jdbc_options())
       .option("dbtable", f"silver.{table}")
       .option("truncate", "true")
       .option("batchsize", "5000")
       .mode("overwrite")
       .save())
    df.unpersist()
    return count


def main() -> None:
    spark = SparkSession.builder.appName("ecommerce-bronze-to-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Everything below is derived from these reads; cache the cleaned entities that feed later joins
    # so bronze is read from Postgres once per table.
    customers, rej_customers = transforms.customers(read_bronze(spark, "customers"))
    products, rej_products = transforms.products(read_bronze(spark, "products"))
    customers, products = customers.cache(), products.cache()
    orders, rej_orders = transforms.orders(read_bronze(spark, "orders"), customers)
    orders = orders.cache()
    items, rej_items = transforms.order_items(read_bronze(spark, "order_items"), orders, products)
    payments, rej_payments = transforms.payments(read_bronze(spark, "payments"), orders)
    quarantine = transforms.union_rejects([rej_customers, rej_products, rej_orders, rej_items, rej_payments])

    counts = {
        "customers": write_silver(customers, "customers"),
        "products": write_silver(products, "products"),
        "orders": write_silver(orders, "orders"),
        "order_items": write_silver(items, "order_items"),
        "payments": write_silver(payments, "payments"),
        "quarantine": write_silver(quarantine, "quarantine"),
    }
    print("silver row counts:", counts)
    for row in quarantine.groupBy("entity", "reason").count().orderBy("entity", "reason").collect():
        print(f"  quarantined {row['entity']}.{row['reason']}: {row['count']}")
    spark.stop()


if __name__ == "__main__":
    main()
