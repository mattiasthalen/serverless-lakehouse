MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    product_description_id,
    description AS product_description__description,
    modified_date AS product_description__modified_date,
    rowguid AS product_description__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_description__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_descriptions")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_description_id ORDER BY product_description__record_loaded_at) AS product_description__record_version,
    CASE
      WHEN product_description__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_description__record_loaded_at
    END AS product_description__record_valid_from,
    COALESCE(
      LEAD(product_description__record_loaded_at) OVER (PARTITION BY product_description_id ORDER BY product_description__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_description__record_valid_to,
    product_description__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_description__is_current_record,
    CASE WHEN product_description__is_current_record THEN product_description__record_loaded_at ELSE product_description__record_valid_to END AS product_description__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_description|adventure_works|', product_description_id, '~epoch|valid_from|', product_description__valid_from)::BLOB AS _pit_hook__product_description,
    CONCAT('product_description|adventure_works|', product_description_id)::BLOB AS _hook__product_description,
    *
  FROM validity
)
SELECT
  *
FROM hooks