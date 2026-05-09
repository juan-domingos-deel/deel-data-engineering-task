TOP_CUSTOMERS_OPEN = """
    SELECT c.customer_id,
           c.customer_name,
           COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders f
    JOIN marts.dim_customer c
      ON c.customer_sk = f.customer_sk
    WHERE f.is_deleted = FALSE
      AND marts.is_open_status(f.current_status)
    GROUP BY c.customer_id, c.customer_name
    ORDER BY order_count DESC, c.customer_id
    LIMIT $1
"""

TOP_CUSTOMERS_LITERAL = """
    SELECT c.customer_id,
           c.customer_name,
           COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders f
    JOIN marts.dim_customer c
      ON c.customer_sk = f.customer_sk
    WHERE f.is_deleted = FALSE
      AND f.current_status = $1
    GROUP BY c.customer_id, c.customer_name
    ORDER BY order_count DESC, c.customer_id
    LIMIT $2
"""
