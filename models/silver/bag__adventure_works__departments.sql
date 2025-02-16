MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    department_id,
    group_name AS department__group_name,
    modified_date AS department__modified_date,
    name AS department__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS department__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__departments")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY department__record_loaded_at) AS department__record_version,
    CASE
      WHEN department__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE department__record_loaded_at
    END AS department__record_valid_from,
    COALESCE(
      LEAD(department__record_loaded_at) OVER (PARTITION BY department_id ORDER BY department__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS department__record_valid_to,
    department__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS department__is_current_record,
    CASE WHEN department__is_current_record THEN department__record_loaded_at ELSE department__record_valid_to END AS department__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('department|adventure_works|', department_id, '~epoch|valid_from|', department__record_valid_from)::BLOB AS _pit_hook__department,
    CONCAT('department|adventure_works|', department_id)::BLOB AS _hook__department,
    *
  FROM validity
)
SELECT
  *
FROM hooks