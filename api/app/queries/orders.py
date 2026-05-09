"""Raw SQL for orders endpoints. ADR-0004 § Endpoint specifications.

`status='open'` is the alias for "everything except COMPLETED" (resolved
plan decision; encoded as marts.is_open_status). Any other literal status
is matched exactly.
"""

ORDERS_BY_DATE_STATUS_OPEN = """
    SELECT delivery_date, current_status AS status, COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders
    WHERE is_deleted = FALSE
      AND marts.is_open_status(current_status)
    GROUP BY delivery_date, current_status
    ORDER BY delivery_date NULLS LAST, current_status
"""

ORDERS_BY_DATE_STATUS_LITERAL = """
    SELECT delivery_date, current_status AS status, COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders
    WHERE is_deleted = FALSE
      AND current_status = $1
    GROUP BY delivery_date, current_status
    ORDER BY delivery_date NULLS LAST, current_status
"""

ORDERS_BY_DATE_ALL = """
    SELECT delivery_date, current_status AS status, COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders
    WHERE is_deleted = FALSE
    GROUP BY delivery_date, current_status
    ORDER BY delivery_date NULLS LAST, current_status
"""

TOP_DELIVERY_DATES_OPEN = """
    SELECT delivery_date, COUNT(*)::BIGINT AS order_count
    FROM marts.fact_orders
    WHERE is_deleted = FALSE
      AND marts.is_open_status(current_status)
      AND delivery_date IS NOT NULL
    GROUP BY delivery_date
    ORDER BY order_count DESC, delivery_date
    LIMIT $1
"""
