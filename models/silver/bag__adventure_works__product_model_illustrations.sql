MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    illustration_id,
    product_model_id,
    modified_date AS illustration__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS illustration__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_model_illustrations")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY illustration_id ORDER BY illustration__record_loaded_at) AS illustration__record_version,
    CASE
      WHEN illustration__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE illustration__record_loaded_at
    END AS illustration__record_valid_from,
    COALESCE(
      LEAD(illustration__record_loaded_at) OVER (PARTITION BY illustration_id ORDER BY illustration__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS illustration__record_valid_to,
    illustration__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS illustration__is_current_record,
    CASE WHEN illustration__is_current_record THEN illustration__record_loaded_at ELSE illustration__record_valid_to END AS illustration__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('illustration|adventure_works|', illustration_id, '~epoch|valid_from|', illustration__record_valid_from)::BLOB AS _pit_hook__illustration,
    CONCAT('illustration|adventure_works|', illustration_id)::BLOB AS _hook__illustration,
    CONCAT('product_model|adventure_works|', product_model_id)::BLOB AS _hook__product_model,
    *
  FROM validity
)
SELECT
  *
FROM hooks