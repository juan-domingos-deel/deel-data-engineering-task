-- Hybrid fact strategy: current-state fact_orders + append-only status events
-- + denormalized fact_order_items. ADR-0003 § Sub-decision B.

CREATE TABLE IF NOT EXISTS marts.fact_orders (
    order_id            BIGINT       NOT NULL PRIMARY KEY,   -- natural key
    customer_sk         BIGINT       NOT NULL REFERENCES marts.dim_customer(customer_sk),
    order_date_key      INTEGER      REFERENCES marts.dim_date(date_key),
    delivery_date_key   INTEGER      REFERENCES marts.dim_date(date_key),
    order_date          DATE,
    delivery_date       DATE,
    current_status      VARCHAR(50)  NOT NULL,               -- degenerate dim
    current_status_at   TIMESTAMPTZ  NOT NULL,
    last_updated_at     TIMESTAMPTZ  NOT NULL,
    is_deleted          BOOLEAN      NOT NULL DEFAULT FALSE,
    deleted_at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS marts.fact_order_items (
    order_item_id          BIGINT       NOT NULL PRIMARY KEY,   -- natural key
    order_id               BIGINT       NOT NULL,
    product_sk             BIGINT       NOT NULL REFERENCES marts.dim_product(product_sk),
    customer_sk            BIGINT       NOT NULL REFERENCES marts.dim_customer(customer_sk),
    quantity               INTEGER,
    current_order_status   VARCHAR(50)  NOT NULL,           -- denormalized from fact_orders
    order_date_key         INTEGER      REFERENCES marts.dim_date(date_key),
    delivery_date_key      INTEGER      REFERENCES marts.dim_date(date_key),
    last_updated_at        TIMESTAMPTZ  NOT NULL,
    is_deleted             BOOLEAN      NOT NULL DEFAULT FALSE,
    deleted_at             TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS marts.fact_order_status_events (
    event_id          BIGSERIAL    PRIMARY KEY,
    order_id          BIGINT       NOT NULL,                -- natural key from source
    status            VARCHAR(50)  NOT NULL,
    status_changed_at TIMESTAMPTZ  NOT NULL,
    event_lsn         BIGINT,                               -- source LSN for deterministic ordering
    UNIQUE (order_id, status_changed_at, status)
);
