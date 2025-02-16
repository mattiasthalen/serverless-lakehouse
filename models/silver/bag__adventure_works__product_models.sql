MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    product_model_id,
    catalog_description AS product_model__catalog_description,
    instructions AS product_model__instructions,
    modified_date AS product_model__modified_date,
    name AS product_model__name,
    rowguid AS product_model__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_model__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_models")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_model_id ORDER BY product_model__record_loaded_at) AS product_model__record_version,
    CASE
      WHEN product_model__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_model__record_loaded_at
    END AS product_model__record_valid_from,
    COALESCE(
      LEAD(product_model__record_loaded_at) OVER (PARTITION BY product_model_id ORDER BY product_model__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_model__record_valid_to,
    product_model__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_model__is_current_record,
    CASE WHEN product_model__is_current_record THEN product_model__record_loaded_at ELSE product_model__record_valid_to END AS product_model__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_model|adventure_works|', product_model_id)::BLOB AS _hook__product_model,
    *
  FROM validity
)
SELECT
  *
FROM hooks