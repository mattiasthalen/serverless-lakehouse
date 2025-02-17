MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    product_id,
    product_model_id,
    product_subcategory_id,
    class AS product__class,
    color AS product__color,
    days_to_manufacture AS product__days_to_manufacture,
    finished_goods_flag AS product__finished_goods_flag,
    list_price AS product__list_price,
    make_flag AS product__make_flag,
    modified_date AS product__modified_date,
    name AS product__name,
    product_line AS product__product_line,
    product_number AS product__product_number,
    reorder_point AS product__reorder_point,
    rowguid AS product__rowguid,
    safety_stock_level AS product__safety_stock_level,
    sell_end_date AS product__sell_end_date,
    sell_start_date AS product__sell_start_date,
    size AS product__size,
    size_unit_measure_code AS product__size_unit_measure_code,
    standard_cost AS product__standard_cost,
    style AS product__style,
    weight AS product__weight,
    weight_unit_measure_code AS product__weight_unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product__record_loaded_at
  FROM bronze.raw__adventure_works__products
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product__record_loaded_at) AS product__record_version,
    CASE
      WHEN product__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product__record_loaded_at
    END AS product__record_valid_from,
    COALESCE(
      LEAD(product__record_loaded_at) OVER (PARTITION BY product_id ORDER BY product__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product__record_valid_to,
    product__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product__is_current_record,
    CASE
      WHEN product__is_current_record
      THEN product__record_loaded_at
      ELSE product__record_valid_to
    END AS product__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product_id,
      '~epoch|valid_from|',
      product__record_valid_from
    )::BLOB AS _pit_hook__product,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('product_model|adventure_works|', product_model_id)::BLOB AS _hook__product_model,
    CONCAT('product_subcategory|adventure_works|', product_subcategory_id)::BLOB AS _hook__product_subcategory,
    *
  FROM validity
)
SELECT
  *
FROM hooks