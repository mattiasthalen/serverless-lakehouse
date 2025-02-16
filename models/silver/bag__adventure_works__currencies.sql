MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    currency_code,
    modified_date AS currency_code__modified_date,
    name AS currency_code__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency_code__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__currencies")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY currency_code__record_loaded_at) AS currency_code__record_version,
    CASE
      WHEN currency_code__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE currency_code__record_loaded_at
    END AS currency_code__record_valid_from,
    COALESCE(
      LEAD(currency_code__record_loaded_at) OVER (PARTITION BY currency_code ORDER BY currency_code__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS currency_code__record_valid_to,
    currency_code__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS currency_code__is_current_record,
    CASE WHEN currency_code__is_current_record THEN currency_code__record_loaded_at ELSE currency_code__record_valid_to END AS currency_code__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('currency_code|adventure_works|', currency_code, '~epoch|valid_from|', currency_code__record_valid_from)::BLOB AS _pit_hook__currency_code,
    CONCAT('currency_code|adventure_works|', currency_code)::BLOB AS _hook__currency_code,
    *
  FROM validity
)
SELECT
  *
FROM hooks