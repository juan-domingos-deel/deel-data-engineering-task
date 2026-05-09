-- marts schema: Kimball star.
-- SCD2 dimensions (customer, product), generated dim_date, status as degenerate dim.
-- ADR-0003 § Schema layout.

CREATE TABLE IF NOT EXISTS marts.dim_customer (
    customer_sk      BIGSERIAL    PRIMARY KEY,
    customer_id      BIGINT       NOT NULL,
    customer_name    VARCHAR(500) NOT NULL,
    customer_address VARCHAR(500),
    is_active        BOOLEAN      NOT NULL,
    valid_from       TIMESTAMPTZ  NOT NULL,
    valid_to         TIMESTAMPTZ  NULL,
    is_current       BOOLEAN      NOT NULL,
    row_hash         BYTEA        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS dim_customer_one_current
    ON marts.dim_customer (customer_id) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS marts.dim_product (
    product_sk       BIGSERIAL    PRIMARY KEY,
    product_id       BIGINT       NOT NULL,
    product_name     VARCHAR(500) NOT NULL,
    barcode          VARCHAR(26)  NOT NULL,
    unity_price      NUMERIC      NOT NULL,
    is_active        BOOLEAN,
    valid_from       TIMESTAMPTZ  NOT NULL,
    valid_to         TIMESTAMPTZ  NULL,
    is_current       BOOLEAN      NOT NULL,
    row_hash         BYTEA        NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS dim_product_one_current
    ON marts.dim_product (product_id) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_key     INTEGER     PRIMARY KEY,           -- YYYYMMDD
    full_date    DATE        NOT NULL UNIQUE,
    year         SMALLINT    NOT NULL,
    quarter      SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    month_name   VARCHAR(12) NOT NULL,
    day          SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,              -- 0 = Sunday … 6 = Saturday
    day_name     VARCHAR(12) NOT NULL,
    is_weekend   BOOLEAN     NOT NULL
);
