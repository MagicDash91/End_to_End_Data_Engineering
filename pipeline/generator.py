"""Fictional Indonesian e-commerce data for the shop (source) database.

`seed` fills an empty shop with history; `simulate` adds a batch of new activity (new customers and
orders, status changes on open orders, price changes) so each pipeline run has something incremental
to pick up. A small share of rows is deliberately dirty -- messy casing and whitespace, phone numbers in
mixed formats, invalid emails, zero quantities, orphan order items -- because cleaning that up is what
the silver layer is for.
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
from decimal import ROUND_HALF_UP, Decimal

from psycopg2.extras import execute_values

from pipeline.db import connect, shop_db

FIRST_NAMES = [
    "Budi", "Siti", "Agus", "Dewi", "Andi", "Rina", "Joko", "Putri", "Rizky", "Ayu", "Hendra", "Fitri",
    "Dimas", "Nur", "Bayu", "Intan", "Fajar", "Lestari", "Yoga", "Maya", "Arif", "Wulan", "Taufik", "Sari",
]
LAST_NAMES = [
    "Santoso", "Wijaya", "Saputra", "Lestari", "Pratama", "Hidayat", "Kusuma", "Nugroho", "Rahmawati",
    "Siregar", "Wibowo", "Halim", "Gunawan", "Susanto", "Purnomo", "Setiawan", None,
]
CITIES = [
    ("Jakarta", "DKI Jakarta"), ("Bandung", "Jawa Barat"), ("Bekasi", "Jawa Barat"),
    ("Surabaya", "Jawa Timur"), ("Semarang", "Jawa Tengah"), ("Yogyakarta", "DI Yogyakarta"),
    ("Medan", "Sumatera Utara"), ("Makassar", "Sulawesi Selatan"), ("Denpasar", "Bali"),
]
# category -> (product names, price range in IDR)
CATALOG = {
    "Electronics": (["Wireless Earbuds", "Power Bank 20000mAh", "Smartwatch", "Bluetooth Speaker",
                     "USB-C Charger 65W", "Mechanical Keyboard"], (150_000, 2_500_000)),
    "Fashion": (["Batik Shirt", "Hijab Voal", "Sneakers", "Denim Jacket", "Sarong Tenun", "Backpack"],
                (75_000, 900_000)),
    "Home": (["Rice Cooker", "Air Fryer", "Bed Sheet Set", "Standing Fan", "Water Dispenser"],
             (120_000, 1_800_000)),
    "Beauty": (["Sunscreen SPF50", "Facial Wash", "Lip Tint", "Serum Niacinamide", "Hair Oil"],
               (35_000, 350_000)),
    "Groceries": (["Kopi Toraja 250g", "Beras Premium 5kg", "Minyak Goreng 2L", "Teh Melati", "Sambal Bawang"],
                  (15_000, 150_000)),
}
PAYMENT_METHODS = ["bank_transfer", "credit_card", "e_wallet", "virtual_account", "cod"]
OPEN_STATUSES = ["pending", "paid", "shipped"]

# Shares of deliberately dirty rows.
DIRTY_EMAIL_FORMAT = 0.10   # cleanable: " Budi.Santoso@Example.COM "
INVALID_EMAIL = 0.02        # quarantined: "budi.santoso#example.com"
MESSY_TEXT = 0.06           # cleanable: "Delivered ", "jakarta  ", "E-Wallet"
BAD_QUANTITY = 0.01         # quarantined: quantity 0
ORPHAN_ITEM = 0.005         # quarantined: order_id that does not exist
ORPHAN_ORDER_OFFSET = 9_000_000


def _money(value: float) -> Decimal:
    return Decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def _next_id(cur, table: str, column: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}")
    return cur.fetchone()[0]


def _phone(rng: random.Random) -> str:
    digits = f"8{rng.randint(11, 59)}{rng.randint(10_000_000, 99_999_999)}"
    style = rng.random()
    if style < 0.5:
        return "0" + digits
    if style < 0.8:
        return f"+62 {digits[:3]}-{digits[3:7]}-{digits[7:]}"
    return "62" + digits


def _email(rng: random.Random, first: str, last: str | None, customer_id: int) -> str:
    local = f"{first}.{last or 'user'}{customer_id}".lower()
    roll = rng.random()
    if roll < INVALID_EMAIL:
        return f"{local}#example.com"
    if roll < INVALID_EMAIL + DIRTY_EMAIL_FORMAT:
        return f"  {first}.{last or 'User'}{customer_id}@Example.COM "
    return f"{local}@example.com"


def _messy(rng: random.Random, value: str) -> str:
    if rng.random() >= MESSY_TEXT:
        return value
    return rng.choice([value.upper(), value.title() + " ", f" {value}", value.replace("_", "-").title()])


def _customer(rng: random.Random, customer_id: int, created: dt.datetime) -> tuple:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    city, province = rng.choice(CITIES)
    city = _messy(rng, city)
    return (customer_id, first, last, _email(rng, first, last, customer_id), _phone(rng), city, province,
            created, created)


def _products(rng: random.Random, start_id: int, created: dt.datetime) -> list[tuple]:
    rows = []
    product_id = start_id
    for category, (names, (low, high)) in CATALOG.items():
        for name in names:
            price = _money(rng.uniform(low, high) / 1000) * 1000
            sku = f"{category[:3].upper()}-{product_id:05d}"
            rows.append((product_id, sku, name, _messy(rng, category.lower()), price, True, created, created))
            product_id += 1
    return rows


def _status_for_age(rng: random.Random, age_days: float) -> str:
    if age_days < 1:
        return rng.choices(["pending", "paid"], [0.6, 0.4])[0]
    if age_days < 4:
        return rng.choices(["paid", "shipped", "cancelled"], [0.3, 0.6, 0.1])[0]
    return rng.choices(["delivered", "cancelled", "returned"], [0.86, 0.09, 0.05])[0]


def _payment_status(order_status: str) -> str:
    return {"pending": "pending", "cancelled": "failed", "returned": "refunded"}.get(order_status, "settled")


def _order_rows(rng, order_id, item_id, payment_id, customer_id, order_date, now, products):
    """One order with 1-4 items and (unless pending) one payment."""
    status = _status_for_age(rng, (now - order_date).total_seconds() / 86400)
    city = rng.choice(CITIES)[0]
    updated = min(order_date + dt.timedelta(days=rng.uniform(0, 6)), now) if status != "pending" else order_date
    order = (order_id, customer_id, _messy(rng, status), order_date, city, order_date, updated)

    items, net = [], Decimal(0)
    for product in rng.sample(products, k=rng.randint(1, 4)):
        quantity = rng.choices([1, 2, 3, 5], [0.65, 0.2, 0.1, 0.05])[0]
        if rng.random() < BAD_QUANTITY:
            quantity = 0
        price = product[4]
        discount = _money(float(price) * quantity * rng.choice([0, 0, 0, 0.05, 0.1]))
        target_order = order_id + ORPHAN_ORDER_OFFSET if rng.random() < ORPHAN_ITEM else order_id
        items.append((item_id, target_order, product[0], quantity, price, discount, order_date, order_date))
        if target_order == order_id:
            net += price * quantity - discount
        item_id += 1

    payments = []
    if status != "pending":
        paid_at = order_date + dt.timedelta(minutes=rng.randint(2, 180))
        payments.append((payment_id, order_id, _messy(rng, rng.choice(PAYMENT_METHODS)), net,
                         _payment_status(status), paid_at, order_date, updated))
        payment_id += 1
    return order, items, payments, item_id, payment_id


def _insert(cur, table: str, columns: str, rows: list[tuple]) -> None:
    if rows:
        execute_values(cur, f"INSERT INTO {table} ({columns}) VALUES %s", rows, page_size=1000)


CUSTOMER_COLS = "customer_id, first_name, last_name, email, phone, city, province, created_at, updated_at"
PRODUCT_COLS = "product_id, sku, name, category, unit_price, is_active, created_at, updated_at"
ORDER_COLS = "order_id, customer_id, order_status, order_date, shipping_city, created_at, updated_at"
ITEM_COLS = "order_item_id, order_id, product_id, quantity, unit_price, discount_amount, created_at, updated_at"
PAYMENT_COLS = "payment_id, order_id, payment_method, amount, status, paid_at, created_at, updated_at"


def _add_orders(cur, rng, n_orders, customer_ids, products, start, now) -> int:
    order_id = _next_id(cur, "orders", "order_id")
    item_id = _next_id(cur, "order_items", "order_item_id")
    payment_id = _next_id(cur, "payments", "payment_id")
    orders, items, payments = [], [], []
    span = (now - start).total_seconds()
    for _ in range(n_orders):
        order_date = start + dt.timedelta(seconds=rng.uniform(0, span))
        order, o_items, o_payments, item_id, payment_id = _order_rows(
            rng, order_id, item_id, payment_id, rng.choice(customer_ids), order_date, now, products)
        orders.append(order)
        items.extend(o_items)
        payments.extend(o_payments)
        order_id += 1
    _insert(cur, "orders", ORDER_COLS, orders)
    _insert(cur, "order_items", ITEM_COLS, items)
    _insert(cur, "payments", PAYMENT_COLS, payments)
    return len(orders)


def seed(conn, n_customers: int = 500, n_orders: int = 3000, days: int = 180, seed_value: int = 42) -> dict:
    rng = random.Random(seed_value)
    now = dt.datetime.now().replace(microsecond=0)
    start = now - dt.timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0]:
            return {"skipped": "shop already has data; use simulate"}
        products = _products(rng, 1, start)
        _insert(cur, "products", PRODUCT_COLS, products)
        customers = [
            _customer(rng, cid, start + dt.timedelta(seconds=rng.uniform(0, (now - start).total_seconds() * 0.7)))
            for cid in range(1, n_customers + 1)
        ]
        _insert(cur, "customers", CUSTOMER_COLS, customers)
        orders = _add_orders(cur, rng, n_orders, [c[0] for c in customers], products, start, now)
    conn.commit()
    return {"products": len(products), "customers": len(customers), "orders": orders}


def simulate(conn, n_orders: int = 150, n_customers: int = 10, seed_value: int | None = None) -> dict:
    """New activity since the last run: new customers/orders, open orders progressing, price changes."""
    rng = random.Random(seed_value)
    now = dt.datetime.now().replace(microsecond=0)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {PRODUCT_COLS} FROM products WHERE is_active")
        products = cur.fetchall()
        if not products:
            raise RuntimeError("shop is empty; run seed first")

        first_id = _next_id(cur, "customers", "customer_id")
        new_customers = [_customer(rng, cid, now) for cid in range(first_id, first_id + n_customers)]
        _insert(cur, "customers", CUSTOMER_COLS, new_customers)
        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT COALESCE(MAX(order_date), %s) FROM orders", (now - dt.timedelta(days=1),))
        since = min(cur.fetchone()[0], now - dt.timedelta(hours=1))
        orders = _add_orders(cur, rng, n_orders, customer_ids, products, since, now)

        # Open orders move one step along the lifecycle.
        cur.execute(
            """
            UPDATE orders SET
                order_status = CASE lower(trim(order_status))
                    WHEN 'pending' THEN 'paid' WHEN 'paid' THEN 'shipped' ELSE 'delivered' END,
                updated_at = %s
            WHERE lower(trim(order_status)) = ANY(%s) AND random() < 0.5
            """,
            (now, OPEN_STATUSES),
        )
        progressed = cur.rowcount

        # A couple of price changes -> new product versions for silver to de-duplicate.
        cur.execute(
            """
            UPDATE products SET unit_price = round(unit_price * (0.9 + random() * 0.2)::numeric, -2), updated_at = %s
            WHERE product_id IN (SELECT product_id FROM products ORDER BY random() LIMIT 2)
            """,
            (now,),
        )
    conn.commit()
    return {"new_customers": len(new_customers), "new_orders": orders, "orders_progressed": progressed}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["seed", "simulate"])
    parser.add_argument("--orders", type=int)
    parser.add_argument("--customers", type=int)
    args = parser.parse_args()
    with connect(shop_db()) as conn:
        if args.command == "seed":
            result = seed(conn, n_customers=args.customers or 500, n_orders=args.orders or 3000)
        else:
            result = simulate(conn, n_orders=args.orders or 150, n_customers=args.customers or 10)
    print(result)


if __name__ == "__main__":
    main()
