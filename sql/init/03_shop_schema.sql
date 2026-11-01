-- Run against the shop (source OLTP) database.
-- Every table has updated_at so Airbyte can sync it incrementally with that column as the cursor.
-- order_items.order_id and payments.order_id deliberately have no foreign key: the "legacy" checkout
-- service writes them separately, so orphan rows can reach the warehouse and silver must catch them.

CREATE TABLE IF NOT EXISTS customers (
    customer_id  BIGINT PRIMARY KEY,
    first_name   TEXT NOT NULL,
    last_name    TEXT,
    email        TEXT,
    phone        TEXT,
    city         TEXT,
    province     TEXT,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id   BIGINT PRIMARY KEY,
    sku          TEXT NOT NULL,
    name         TEXT NOT NULL,
    category     TEXT,
    unit_price   NUMERIC(14, 2) NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       BIGINT PRIMARY KEY,
    customer_id    BIGINT REFERENCES customers (customer_id),
    order_status   TEXT NOT NULL,
    order_date     TIMESTAMP NOT NULL,
    shipping_city  TEXT,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id    BIGINT PRIMARY KEY,
    order_id         BIGINT NOT NULL,
    product_id       BIGINT NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price       NUMERIC(14, 2) NOT NULL,
    discount_amount  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id      BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    payment_method  TEXT NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    status          TEXT NOT NULL,
    paid_at         TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at   ON customers (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at    ON products (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at      ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON order_items (updated_at);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at    ON payments (updated_at);
