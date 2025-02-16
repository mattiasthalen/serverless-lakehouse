MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    product_id,
    end_date AS product__end_date,
    modified_date AS product__modified_date,
    standard_cost AS product__standard_cost,
    start_date AS product__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_cost_histories")
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
    CASE WHEN product__is_current_record THEN product__record_loaded_at ELSE product__record_valid_to END AS product__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product|adventure_works|', product_id, '~epoch|valid_from|', product__record_valid_from)::BLOB AS _pit_hook__product,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    *
  FROM validity
)
SELECT
  *
FROM hooks