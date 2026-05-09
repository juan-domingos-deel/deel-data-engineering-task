-- staging schema: 1:1 mirror of source operations.* tables, current state only.
-- Populated by the consumer via UPSERT on every Debezium change event.
-- ADR-0002 § Concrete schema layout.

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id      BIGINT       NOT NULL PRIMARY KEY,
    customer_name    VARCHAR(500) NOT NULL,
    is_active        BOOLEAN      NOT NULL,
    customer_address VARCHAR(500),
    updated_at       TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3),
    created_by       BIGINT,
    source_ts_ms     BIGINT       NOT NULL,
    is_deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id       BIGINT       NOT NULL PRIMARY KEY,
    product_name     VARCHAR(500) NOT NULL,
    barcode          VARCHAR(26)  NOT NULL,
    unity_price      NUMERIC      NOT NULL,
    is_active        BOOLEAN,
    updated_at       TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3),
    created_by       BIGINT,
    source_ts_ms     BIGINT       NOT NULL,
    is_deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id         BIGINT       NOT NULL PRIMARY KEY,
    order_date       DATE,
    delivery_date    DATE,
    customer_id      BIGINT,
    status           VARCHAR(50),
    updated_at       TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3),
    created_by       BIGINT,
    source_ts_ms     BIGINT       NOT NULL,
    source_lsn       BIGINT,
    is_deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_item_id    BIGINT       NOT NULL PRIMARY KEY,
    order_id         BIGINT,
    product_id       BIGINT,
    quantity         INTEGER,
    updated_at       TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3),
    created_by       BIGINT,
    source_ts_ms     BIGINT       NOT NULL,
    is_deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS stg_orders_customer_idx ON staging.stg_orders (customer_id);
CREATE INDEX IF NOT EXISTS stg_order_items_order_idx ON staging.stg_order_items (order_id);
CREATE INDEX IF NOT EXISTS stg_order_items_product_idx ON staging.stg_order_items (product_id);
