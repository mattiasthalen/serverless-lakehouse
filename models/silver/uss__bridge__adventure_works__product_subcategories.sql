MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'product_subcategories' AS stage,
    bag__adventure_works__product_subcategories._pit_hook__product_subcategory,
    bag__adventure_works__product_subcategories._hook__product_subcategory,
    uss__bridge__adventure_works__product_categories._pit_hook__product_category,
    GREATEST(
        bag__adventure_works__product_subcategories.product_subcategory__record_loaded_at,
        uss__bridge__adventure_works__product_categories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        bag__adventure_works__product_subcategories.product_subcategory__record_updated_at,
        uss__bridge__adventure_works__product_categories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        bag__adventure_works__product_subcategories.product_subcategory__record_valid_from,
        uss__bridge__adventure_works__product_categories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        bag__adventure_works__product_subcategories.product_subcategory__record_valid_to,
        uss__bridge__adventure_works__product_categories.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__product_subcategories
  LEFT JOIN silver.uss__bridge__adventure_works__product_categories
    ON bag__adventure_works__product_subcategories._hook__product_category = uss__bridge__adventure_works__product_categories._hook__product_category
    AND bag__adventure_works__product_subcategories.product_subcategory__record_valid_from <= uss__bridge__adventure_works__product_categories.bridge__record_valid_to
    AND bag__adventure_works__product_subcategories.product_subcategory__record_valid_to >= uss__bridge__adventure_works__product_categories.bridge__record_valid_from
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge