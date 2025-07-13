CREATE SCHEMA IF NOT EXISTS monitoring;

-- Output table for Number of open orders by DELIVERY_DATE and STATUS and Top 3 delivery dates with more open orders
CREATE TABLE IF NOT EXISTS monitoring.open_orders(
    delivery_date       DATE,
    status              VARCHAR(32),
    open_order_count    BIGINT,
    processed_at        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- Output table for Number of open pending items by PRODUCT_ID
CREATE TABLE IF NOT EXISTS monitoring.pending_items(
    product_id              BIGSERIAL NOT NULL PRIMARY KEY,
    pending_item_count      BIGINT
    processed_at            TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
    );

-- Output table for Top 3 Customers with more pending orders
CREATE TABLE IF NOT EXISTS monitoring.customers(
    customer_id             BIGINT,
    pending_order_count     BIGINT,
    processed_at            TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

