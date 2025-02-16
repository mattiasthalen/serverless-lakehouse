MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    shift_id,
    end_time AS shift__end_time,
    modified_date AS shift__modified_date,
    name AS shift__name,
    start_time AS shift__start_time,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shift__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__shifts")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shift_id ORDER BY shift__record_loaded_at) AS shift__record_version,
    CASE
      WHEN shift__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE shift__record_loaded_at
    END AS shift__record_valid_from,
    COALESCE(
      LEAD(shift__record_loaded_at) OVER (PARTITION BY shift_id ORDER BY shift__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS shift__record_valid_to,
    shift__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS shift__is_current_record,
    CASE WHEN shift__is_current_record THEN shift__record_loaded_at ELSE shift__record_valid_to END AS shift__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('shift|adventure_works|', shift_id)::BLOB AS _hook__shift,
    *
  FROM validity
)
SELECT
  *
FROM hooks