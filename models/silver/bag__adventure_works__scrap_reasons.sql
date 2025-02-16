MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    scrap_reason_id,
    modified_date AS scrap_reason__modified_date,
    name AS scrap_reason__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS scrap_reason__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__scrap_reasons")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY scrap_reason_id ORDER BY scrap_reason__record_loaded_at) AS scrap_reason__record_version,
    CASE
      WHEN scrap_reason__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE scrap_reason__record_loaded_at
    END AS scrap_reason__record_valid_from,
    COALESCE(
      LEAD(scrap_reason__record_loaded_at) OVER (PARTITION BY scrap_reason_id ORDER BY scrap_reason__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS scrap_reason__record_valid_to,
    scrap_reason__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS scrap_reason__is_current_record,
    CASE WHEN scrap_reason__is_current_record THEN scrap_reason__record_loaded_at ELSE scrap_reason__record_valid_to END AS scrap_reason__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('scrap_reason|adventure_works|', scrap_reason_id, '~epoch|valid_from|', scrap_reason__record_valid_from)::BLOB AS _pit_hook__scrap_reason,
    CONCAT('scrap_reason|adventure_works|', scrap_reason_id)::BLOB AS _hook__scrap_reason,
    *
  FROM validity
)
SELECT
  *
FROM hooks