MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    bag__adventure_works__products._hook__product,
    bag__adventure_works__products.product__class,
    bag__adventure_works__products.product__color,
    bag__adventure_works__products.product__days_to_manufacture,
    bag__adventure_works__products.product__finished_goods_flag,
    bag__adventure_works__products.product__list_price,
    bag__adventure_works__products.product__make_flag,
    bag__adventure_works__products.product__modified_date,
    bag__adventure_works__products.product__name,
    bag__adventure_works__products.product__product_line,
    bag__adventure_works__products.product__product_number,
    bag__adventure_works__products.product__reorder_point,
    bag__adventure_works__products.product__rowguid,
    bag__adventure_works__products.product__safety_stock_level,
    bag__adventure_works__products.product__sell_end_date,
    bag__adventure_works__products.product__sell_start_date,
    bag__adventure_works__products.product__size,
    bag__adventure_works__products.product__size_unit_measure_code,
    bag__adventure_works__products.product__standard_cost,
    bag__adventure_works__products.product__style,
    bag__adventure_works__products.product__weight,
    bag__adventure_works__products.product__weight_unit_measure_code,
    bag__adventure_works__product_subcategories.product_subcategory__modified_date,
    bag__adventure_works__product_subcategories.product_subcategory__name,
    bag__adventure_works__product_subcategories.product_subcategory__rowguid,
    bag__adventure_works__product_categories.product_category__modified_date,
    bag__adventure_works__product_categories.product_category__name,
    bag__adventure_works__product_categories.product_category__rowguid,
    GREATEST(
      bag__adventure_works__products.product__record_loaded_at,
      bag__adventure_works__product_subcategories.product_subcategory__record_loaded_at,
      bag__adventure_works__product_categories.product_category__record_loaded_at
    ) AS product__record_loaded_at,
    GREATEST(
      bag__adventure_works__products.product__record_updated_at,
      bag__adventure_works__product_subcategories.product_subcategory__record_updated_at,
      bag__adventure_works__product_categories.product_category__record_updated_at
    ) AS product__record_updated_at,
    GREATEST(
      bag__adventure_works__products.product__record_valid_from,
      bag__adventure_works__product_subcategories.product_subcategory__record_valid_from,
      bag__adventure_works__product_categories.product_category__record_valid_from
    ) AS product__record_valid_from,
    LEAST(
      bag__adventure_works__products.product__record_valid_to,
      bag__adventure_works__product_subcategories.product_subcategory__record_valid_to,
      bag__adventure_works__product_categories.product_category__record_valid_to
    ) AS product__record_valid_to,
    product__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product__is_current_record
  FROM silver.bag__adventure_works__products
  LEFT JOIN silver.bag__adventure_works__product_subcategories
    ON bag__adventure_works__products._hook__product_subcategory = bag__adventure_works__product_subcategories._hook__product_subcategory
    AND bag__adventure_works__products.product__record_valid_from < bag__adventure_works__product_subcategories.product_subcategory__record_valid_to
    AND bag__adventure_works__products.product__record_valid_to > bag__adventure_works__product_subcategories.product_subcategory__record_valid_from
  LEFT JOIN silver.bag__adventure_works__product_categories
    ON bag__adventure_works__product_subcategories._hook__product_category = bag__adventure_works__product_categories._hook__product_category
    AND bag__adventure_works__products.product__record_valid_from < bag__adventure_works__product_categories.product_category__record_valid_to
    AND bag__adventure_works__products.product__record_valid_to > bag__adventure_works__product_categories.product_category__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(_hook__product::TEXT, '~epoch|valid_from', product__record_valid_from)::BLOB AS _pit_hook__product,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time