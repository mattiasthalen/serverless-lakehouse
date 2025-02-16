MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    product_subcategory_id,
    product_category_id,
    modified_date AS product_subcategory__modified_date,
    name AS product_subcategory__name,
    rowguid AS product_subcategory__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_subcategory__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_subcategories")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_subcategory_id ORDER BY product_subcategory__record_loaded_at) AS product_subcategory__record_version,
    CASE
      WHEN product_subcategory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_subcategory__record_loaded_at
    END AS product_subcategory__record_valid_from,
    COALESCE(
      LEAD(product_subcategory__record_loaded_at) OVER (PARTITION BY product_subcategory_id ORDER BY product_subcategory__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_subcategory__record_valid_to,
    product_subcategory__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_subcategory__is_current_record,
    CASE WHEN product_subcategory__is_current_record THEN product_subcategory__record_loaded_at ELSE product_subcategory__record_valid_to END AS product_subcategory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_subcategory|adventure_works|', product_subcategory_id, '~epoch|valid_from|', product_subcategory__valid_from)::BLOB AS _pit_hook__product_subcategory,
    CONCAT('product_subcategory|adventure_works|', product_subcategory_id)::BLOB AS _hook__product_subcategory,
    CONCAT('product_category|adventure_works|', product_category_id)::BLOB AS _hook__product_category,
    *
  FROM validity
)
SELECT
  *
FROM hooks