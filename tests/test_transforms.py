"""Silver rules on small synthetic bronze frames (local Spark)."""
from datetime import datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
import transforms  # noqa: E402  (spark/jobs is on the pytest pythonpath)

T0 = datetime(2026, 10, 1, 8, 0)
T1 = datetime(2026, 10, 2, 8, 0)
EXTRACTED = datetime(2026, 10, 3, 0, 0)

CUSTOMER_SCHEMA = ("customer_id long, first_name string, last_name string, email string, phone string, "
                   "city string, province string, created_at timestamp, updated_at timestamp, "
                   "_airbyte_extracted_at timestamp")
PRODUCT_SCHEMA = ("product_id long, sku string, name string, category string, unit_price decimal(14,2), "
                  "is_active boolean, created_at timestamp, updated_at timestamp, _airbyte_extracted_at timestamp")
ORDER_SCHEMA = ("order_id long, customer_id long, order_status string, order_date timestamp, "
                "shipping_city string, created_at timestamp, updated_at timestamp, _airbyte_extracted_at timestamp")
ITEM_SCHEMA = ("order_item_id long, order_id long, product_id long, quantity int, unit_price decimal(14,2), "
               "discount_amount decimal(14,2), created_at timestamp, updated_at timestamp, "
               "_airbyte_extracted_at timestamp")
PAYMENT_SCHEMA = ("payment_id long, order_id long, payment_method string, amount decimal(14,2), status string, "
                  "paid_at timestamp, created_at timestamp, updated_at timestamp, _airbyte_extracted_at timestamp")


def customer(cid, email="budi@example.com", phone="081234567890", city="Jakarta", updated=T0, first="budi"):
    return (cid, first, "santoso", email, phone, city, "DKI Jakarta", T0, updated, EXTRACTED)


@pytest.fixture
def silver_customers(spark):
    bronze = spark.createDataFrame([customer(1), customer(2)], CUSTOMER_SCHEMA)
    return transforms.customers(bronze)[0]


@pytest.fixture
def silver_products(spark):
    bronze = spark.createDataFrame(
        [(10, " ele-00010 ", "Smartwatch", "electronics", Decimal("1500000"), True, T0, T0, EXTRACTED)],
        PRODUCT_SCHEMA)
    return transforms.products(bronze)[0]


def test_latest_version_wins(spark):
    bronze = spark.createDataFrame(
        [customer(1, city="Bandung", updated=T0), customer(1, city="Surabaya", updated=T1)], CUSTOMER_SCHEMA)
    silver, rejects = transforms.customers(bronze)
    rows = silver.collect()
    assert len(rows) == 1 and rows[0]["city"] == "Surabaya"
    assert rejects.count() == 0


def test_customer_cleaning(spark):
    bronze = spark.createDataFrame([
        customer(1, email="  Budi.Santoso1@Example.COM ", phone="+62 812-3456-7890", city=" jakarta  "),
        customer(2, email="budi.santoso2#example.com", phone="6281234567890"),
        customer(3, phone="12345"),
    ], CUSTOMER_SCHEMA)
    rows = {r["customer_id"]: r for r in transforms.customers(bronze)[0].collect()}
    assert rows[1]["email"] == "budi.santoso1@example.com" and rows[1]["email_is_valid"]
    assert rows[1]["phone"] == "+6281234567890"
    assert rows[1]["city"] == "Jakarta" and rows[1]["first_name"] == "Budi"
    # A bad email is nulled and flagged, the customer is kept.
    assert rows[2]["email"] is None and rows[2]["email_is_valid"] is False
    assert rows[2]["phone"] == "+6281234567890"
    assert rows[3]["phone"] is None


def test_order_status_normalised_and_invalid_quarantined(spark, silver_customers):
    bronze = spark.createDataFrame([
        (100, 1, "Delivered ", T0, "jakarta", T0, T0, EXTRACTED),
        (101, 1, "CANCELED", T0, "Bandung", T0, T0, EXTRACTED),
        (102, 2, "lost_in_space", T0, "Bandung", T0, T0, EXTRACTED),
        (103, 99, "paid", T0, "Bandung", T0, T0, EXTRACTED),
    ], ORDER_SCHEMA)
    silver, rejects = transforms.orders(bronze, silver_customers)
    statuses = {r["order_id"]: r["order_status"] for r in silver.collect()}
    assert statuses == {100: "delivered", 101: "cancelled"}
    reasons = {r["record_key"]: r["reason"] for r in rejects.collect()}
    assert reasons == {"102": "invalid_status", "103": "unknown_customer"}


def test_order_items_rules(spark, silver_customers, silver_products):
    orders = transforms.orders(
        spark.createDataFrame([(100, 1, "paid", T0, "Jakarta", T0, T0, EXTRACTED)], ORDER_SCHEMA),
        silver_customers)[0]
    bronze = spark.createDataFrame([
        (1, 100, 10, 2, Decimal("1500000"), Decimal("150000"), T0, T0, EXTRACTED),
        (2, 100, 10, 0, Decimal("1500000"), Decimal("0"), T0, T0, EXTRACTED),
        (3, 9_000_100, 10, 1, Decimal("1500000"), Decimal("0"), T0, T0, EXTRACTED),
        (4, 100, 77, 1, Decimal("1000"), Decimal("0"), T0, T0, EXTRACTED),
        (5, 100, 10, 1, Decimal("1000"), Decimal("5000"), T0, T0, EXTRACTED),
    ], ITEM_SCHEMA)
    silver, rejects = transforms.order_items(bronze, orders, silver_products)
    [line] = silver.collect()
    assert line["order_item_id"] == 1
    assert line["gross_amount"] == Decimal("3000000.00") and line["net_amount"] == Decimal("2850000.00")
    reasons = {r["record_key"]: r["reason"] for r in rejects.collect()}
    assert reasons == {"2": "non_positive_quantity", "3": "orphan_order", "4": "unknown_product",
                       "5": "invalid_discount"}


def test_payment_vocab(spark, silver_customers):
    orders = transforms.orders(
        spark.createDataFrame([(100, 1, "paid", T0, "Jakarta", T0, T0, EXTRACTED)], ORDER_SCHEMA),
        silver_customers)[0]
    bronze = spark.createDataFrame([
        (1, 100, "E-Wallet", Decimal("100"), "Settled", T0, T0, T0, EXTRACTED),
        (2, 100, " BANK_TRANSFER", Decimal("100"), "settled", T0, T0, T0, EXTRACTED),
        (3, 100, "barter", Decimal("100"), "settled", T0, T0, T0, EXTRACTED),
    ], PAYMENT_SCHEMA)
    silver, rejects = transforms.payments(bronze, orders)
    methods = {r["payment_id"]: (r["payment_method"], r["status"]) for r in silver.collect()}
    assert methods == {1: ("e_wallet", "settled"), 2: ("bank_transfer", "settled")}
    assert [r["reason"] for r in rejects.collect()] == ["invalid_method"]


def test_quarantine_payload_is_json(spark):
    bronze = spark.createDataFrame(
        [(10, None, "Thing", "home", Decimal("0"), True, T0, T0, EXTRACTED)], PRODUCT_SCHEMA)
    _, rejects = transforms.products(bronze)
    quarantine = transforms.union_rejects([rejects])
    [row] = quarantine.collect()
    assert row["reason"] == "missing_sku" and row["entity"] == "products"
    assert '"name":"Thing"' in row["payload"] and row["quarantined_at"] is not None
