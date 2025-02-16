MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    shopping_cart_item_id,
    product_id,
    shopping_cart_id,
    date_created AS shopping_cart_item__date_created,
    modified_date AS shopping_cart_item__modified_date,
    quantity AS shopping_cart_item__quantity,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shopping_cart_item__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__shopping_cart_items")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at) AS shopping_cart_item__record_version,
    CASE
      WHEN shopping_cart_item__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE shopping_cart_item__record_loaded_at
    END AS shopping_cart_item__record_valid_from,
    COALESCE(
      LEAD(shopping_cart_item__record_loaded_at) OVER (PARTITION BY shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS shopping_cart_item__record_valid_to,
    shopping_cart_item__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS shopping_cart_item__is_current_record,
    CASE WHEN shopping_cart_item__is_current_record THEN shopping_cart_item__record_loaded_at ELSE shopping_cart_item__record_valid_to END AS shopping_cart_item__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('shopping_cart_item|adventure_works|', shopping_cart_item_id)::BLOB AS _hook__shopping_cart_item,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('shopping_cart|adventure_works|', shopping_cart_id)::BLOB AS _hook__shopping_cart,
    *
  FROM validity
)
SELECT
  *
FROM hooks