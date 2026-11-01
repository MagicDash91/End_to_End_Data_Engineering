"""Bronze -> silver rules, one function per entity.

Every function takes the raw bronze DataFrame (all appended versions of every row) and returns
`(silver_df, rejects_df)`:
  * de-duplicate to the latest version of each key (updated_at, then Airbyte extraction time)
  * clean: trim/collapse whitespace, normalise casing, phone numbers to +62..., status/method vocabularies
  * validate: rows breaking a rule go to the quarantine with the first rule they broke

Fixable problems are fixed (messy casing, phone formats). Problems that would make a row wrong in gold
(zero quantity, unknown order, invalid status) are quarantined, not guessed at. A bad email does not drop
the customer: it is nulled and flagged with email_is_valid = false.

Only JVM-side DataFrame expressions are used (no Python UDFs), so executors never import this module.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$"
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "returned"]
STATUS_ALIASES = {"canceled": "cancelled", "complete": "delivered", "completed": "delivered"}
PAYMENT_METHODS = ["bank_transfer", "credit_card", "e_wallet", "virtual_account", "cod"]
PAYMENT_STATUSES = ["pending", "settled", "failed", "refunded"]


def latest_version(df: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_airbyte_extracted_at").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def clean_text(col: str | Column) -> Column:
    col = F.col(col) if isinstance(col, str) else col
    cleaned = F.trim(F.regexp_replace(col, r"\s+", " "))
    return F.when(cleaned == "", None).otherwise(cleaned)


def normalize_phone(col: str) -> Column:
    """0812..., 62812..., +62 812-... and 812... all become +62812...; anything else becomes null."""
    digits = F.regexp_replace(F.col(col), r"[^0-9]", "")
    national = (
        F.when(digits.startswith("62"), digits.substr(F.lit(3), F.length(digits)))
        .when(digits.startswith("0"), digits.substr(F.lit(2), F.length(digits)))
        .otherwise(digits)
    )
    return F.when(national.rlike(r"^8[0-9]{8,11}$"), F.concat(F.lit("+62"), national))


def normalize_vocab(col: str, aliases: dict[str, str] | None = None) -> Column:
    value = F.regexp_replace(F.lower(F.trim(F.col(col))), r"[\s-]+", "_")
    for alias, canonical in (aliases or {}).items():
        value = F.when(value == alias, F.lit(canonical)).otherwise(value)
    return value


def with_reference_flag(df: DataFrame, key: str, ref: DataFrame, ref_key: str, flag: str) -> DataFrame:
    """Adds boolean `flag`: true when df[key] exists in ref[ref_key]."""
    keys = ref.select(F.col(ref_key).alias(key)).distinct().withColumn(flag, F.lit(True))
    return df.join(keys, on=key, how="left").withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))


def split_valid(df: DataFrame, entity: str, key: str, rules: list[tuple[str, Column]],
                drop: list[str] | None = None) -> tuple[DataFrame, DataFrame]:
    """rules are (reason, condition that is TRUE for a bad row); the first matching reason wins."""
    drop = drop or []
    reason = F.coalesce(*[F.when(bad, F.lit(name)) for name, bad in rules])
    flagged = df.withColumn("_dq_reason", reason)

    valid = flagged.filter(F.col("_dq_reason").isNull()).drop("_dq_reason", *drop)
    payload_cols = [c for c in df.columns if c not in drop and not c.startswith("_airbyte")]
    rejects = flagged.filter(F.col("_dq_reason").isNotNull()).select(
        F.lit(entity).alias("entity"),
        F.col(key).cast("string").alias("record_key"),
        F.col("_dq_reason").alias("reason"),
        F.to_json(F.struct(*payload_cols)).alias("payload"),
    )
    return valid, rejects


def _finish(valid: DataFrame) -> DataFrame:
    """Rename the bronze extraction time to the silver lineage column, last."""
    columns = [c for c in valid.columns if c != "_airbyte_extracted_at"]
    return valid.select(*columns, F.col("_airbyte_extracted_at").alias("_source_extracted_at"))


def customers(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    email = F.lower(F.trim(F.col("email")))
    email_ok = F.coalesce(email.rlike(EMAIL_RE), F.lit(False))
    df = latest_version(bronze, "customer_id").select(
        F.col("customer_id").cast("long"),
        F.initcap(clean_text("first_name")).alias("first_name"),
        F.initcap(clean_text("last_name")).alias("last_name"),
        F.when(email_ok, email).alias("email"),
        email_ok.alias("email_is_valid"),
        normalize_phone("phone").alias("phone"),
        F.initcap(clean_text("city")).alias("city"),
        clean_text("province").alias("province"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
        F.col("_airbyte_extracted_at"),
    )
    rules = [
        ("missing_customer_id", F.col("customer_id").isNull()),
        ("missing_first_name", F.col("first_name").isNull()),
    ]
    valid, rejects = split_valid(df, "customers", "customer_id", rules)
    return _finish(valid), rejects


def products(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = latest_version(bronze, "product_id").select(
        F.col("product_id").cast("long"),
        F.upper(clean_text("sku")).alias("sku"),
        clean_text("name").alias("name"),
        F.initcap(F.lower(clean_text("category"))).alias("category"),
        F.col("unit_price").cast("decimal(14,2)"),
        F.col("is_active").cast("boolean"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
        F.col("_airbyte_extracted_at"),
    )
    rules = [
        ("missing_sku", F.col("sku").isNull()),
        ("non_positive_price", F.col("unit_price").isNull() | (F.col("unit_price") <= 0)),
    ]
    valid, rejects = split_valid(df, "products", "product_id", rules)
    return _finish(valid), rejects


def orders(bronze: DataFrame, silver_customers: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = latest_version(bronze, "order_id").select(
        F.col("order_id").cast("long"),
        F.col("customer_id").cast("long"),
        normalize_vocab("order_status", STATUS_ALIASES).alias("order_status"),
        F.col("order_date").cast("timestamp"),
        F.initcap(clean_text("shipping_city")).alias("shipping_city"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
        F.col("_airbyte_extracted_at"),
    )
    df = with_reference_flag(df, "customer_id", silver_customers, "customer_id", "_known_customer")
    rules = [
        ("invalid_status", F.col("order_status").isNull() | ~F.col("order_status").isin(ORDER_STATUSES)),
        ("missing_customer", F.col("customer_id").isNull()),
        ("unknown_customer", ~F.col("_known_customer")),
        ("future_order_date", F.col("order_date") > F.current_timestamp() + F.expr("INTERVAL 1 DAY")),
    ]
    valid, rejects = split_valid(df, "orders", "order_id", rules, drop=["_known_customer"])
    return _finish(valid), rejects


def order_items(bronze: DataFrame, silver_orders: DataFrame,
                silver_products: DataFrame) -> tuple[DataFrame, DataFrame]:
    quantity = F.col("quantity").cast("int")
    unit_price = F.col("unit_price").cast("decimal(14,2)")
    discount = F.coalesce(F.col("discount_amount").cast("decimal(14,2)"), F.lit(0).cast("decimal(14,2)"))
    gross = (unit_price * quantity).cast("decimal(16,2)")
    df = latest_version(bronze, "order_item_id").select(
        F.col("order_item_id").cast("long"),
        F.col("order_id").cast("long"),
        F.col("product_id").cast("long"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price"),
        discount.alias("discount_amount"),
        gross.alias("gross_amount"),
        (gross - discount).cast("decimal(16,2)").alias("net_amount"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
        F.col("_airbyte_extracted_at"),
    )
    df = with_reference_flag(df, "order_id", silver_orders, "order_id", "_known_order")
    df = with_reference_flag(df, "product_id", silver_products, "product_id", "_known_product")
    rules = [
        ("non_positive_quantity", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
        ("negative_price", F.col("unit_price").isNull() | (F.col("unit_price") < 0)),
        ("invalid_discount", (F.col("discount_amount") < 0) | (F.col("discount_amount") > F.col("gross_amount"))),
        ("orphan_order", ~F.col("_known_order")),
        ("unknown_product", ~F.col("_known_product")),
    ]
    valid, rejects = split_valid(df, "order_items", "order_item_id", rules,
                                 drop=["_known_order", "_known_product"])
    return _finish(valid), rejects


def payments(bronze: DataFrame, silver_orders: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = latest_version(bronze, "payment_id").select(
        F.col("payment_id").cast("long"),
        F.col("order_id").cast("long"),
        normalize_vocab("payment_method").alias("payment_method"),
        F.col("amount").cast("decimal(14,2)"),
        normalize_vocab("status").alias("status"),
        F.col("paid_at").cast("timestamp"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
        F.col("_airbyte_extracted_at"),
    )
    df = with_reference_flag(df, "order_id", silver_orders, "order_id", "_known_order")
    rules = [
        ("invalid_method", F.col("payment_method").isNull() | ~F.col("payment_method").isin(PAYMENT_METHODS)),
        ("invalid_status", F.col("status").isNull() | ~F.col("status").isin(PAYMENT_STATUSES)),
        ("negative_amount", F.col("amount").isNull() | (F.col("amount") < 0)),
        ("orphan_order", ~F.col("_known_order")),
    ]
    valid, rejects = split_valid(df, "payments", "payment_id", rules, drop=["_known_order"])
    return _finish(valid), rejects


def union_rejects(rejects: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b), rejects).withColumn("quarantined_at", F.current_timestamp())
