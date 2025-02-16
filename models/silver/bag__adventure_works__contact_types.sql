MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    contact_type_id,
    modified_date AS contact_type__modified_date,
    name AS contact_type__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS contact_type__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__contact_types")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY contact_type_id ORDER BY contact_type__record_loaded_at) AS contact_type__record_version,
    CASE
      WHEN contact_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE contact_type__record_loaded_at
    END AS contact_type__record_valid_from,
    COALESCE(
      LEAD(contact_type__record_loaded_at) OVER (PARTITION BY contact_type_id ORDER BY contact_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS contact_type__record_valid_to,
    contact_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS contact_type__is_current_record,
    CASE WHEN contact_type__is_current_record THEN contact_type__record_loaded_at ELSE contact_type__record_valid_to END AS contact_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('contact_type|adventure_works|', contact_type_id, '~epoch|valid_from|', contact_type__record_valid_from)::BLOB AS _pit_hook__contact_type,
    CONCAT('contact_type|adventure_works|', contact_type_id)::BLOB AS _hook__contact_type,
    *
  FROM validity
)
SELECT
  *
FROM hooks