-- Required indexes per ADR-0003 § Required indexes.
-- Partial-index predicates `WHERE is_deleted = false` keep soft-deleted rows
-- off the hot read path (per resolved decision in plan).

CREATE INDEX IF NOT EXISTS dim_customer_lookup
    ON marts.dim_customer (customer_id, is_current);

CREATE INDEX IF NOT EXISTS dim_product_lookup
    ON marts.dim_product (product_id, is_current);

CREATE INDEX IF NOT EXISTS fact_orders_status_delivery
    ON marts.fact_orders (current_status, delivery_date_key)
    WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS fact_orders_status_customer
    ON marts.fact_orders (current_status, customer_sk)
    WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS fact_orders_customer
    ON marts.fact_orders (customer_sk)
    WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS fact_order_items_status_product
    ON marts.fact_order_items (current_order_status, product_sk)
    WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS fact_order_items_order
    ON marts.fact_order_items (order_id);

CREATE INDEX IF NOT EXISTS fact_order_status_events_order
    ON marts.fact_order_status_events (order_id, status_changed_at);
