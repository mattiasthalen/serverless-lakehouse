MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    culture_id,
    modified_date AS culture__modified_date,
    name AS culture__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS culture__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__cultures")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY culture_id ORDER BY culture__record_loaded_at) AS culture__record_version,
    CASE
      WHEN culture__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE culture__record_loaded_at
    END AS culture__record_valid_from,
    COALESCE(
      LEAD(culture__record_loaded_at) OVER (PARTITION BY culture_id ORDER BY culture__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS culture__record_valid_to,
    culture__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS culture__is_current_record,
    CASE WHEN culture__is_current_record THEN culture__record_loaded_at ELSE culture__record_valid_to END AS culture__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('culture|adventure_works|', culture_id, '~epoch|valid_from|', culture__record_valid_from)::BLOB AS _pit_hook__culture,
    CONCAT('culture|adventure_works|', culture_id)::BLOB AS _hook__culture,
    *
  FROM validity
)
SELECT
  *
FROM hooks