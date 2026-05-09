OPEN_ITEMS_BY_PRODUCT = """
    SELECT p.product_id,
           p.product_name,
           COUNT(*)::BIGINT AS open_item_count
    FROM marts.fact_order_items i
    JOIN marts.dim_product p
      ON p.product_sk = i.product_sk
    WHERE i.is_deleted = FALSE
      AND marts.is_open_status(i.current_order_status)
    GROUP BY p.product_id, p.product_name
    ORDER BY open_item_count DESC, p.product_id
    LIMIT $1 OFFSET $2
"""
