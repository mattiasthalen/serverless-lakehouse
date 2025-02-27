MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'products' AS stage,
    bag__adventure_works__products._pit_hook__product,
    bag__adventure_works__products._hook__product,
    uss_bridge__product_subcategories._pit_hook__product_subcategory,
    uss_bridge__product_subcategories._pit_hook__product_category,
    GREATEST(
      bag__adventure_works__products.product__record_loaded_at,
      uss_bridge__product_subcategories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
      bag__adventure_works__products.product__record_updated_at,
      uss_bridge__product_subcategories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
      bag__adventure_works__products.product__record_valid_from,
      uss_bridge__product_subcategories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
      bag__adventure_works__products.product__record_valid_to,
      uss_bridge__product_subcategories.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__products
  LEFT JOIN silver.uss_bridge__product_subcategories
    ON bag__adventure_works__products._hook__product_subcategory = uss_bridge__product_subcategories._hook__product_subcategory
    AND bag__adventure_works__products.product__record_valid_from <= uss_bridge__product_subcategories.bridge__record_valid_to
    AND bag__adventure_works__products.product__record_valid_to >= uss_bridge__product_subcategories.bridge__record_valid_from
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge