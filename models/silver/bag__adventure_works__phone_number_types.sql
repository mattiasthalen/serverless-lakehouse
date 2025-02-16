MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    phone_number_type_id,
    modified_date AS phone_number_type__modified_date,
    name AS phone_number_type__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS phone_number_type__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__phone_number_types")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY phone_number_type_id ORDER BY phone_number_type__record_loaded_at) AS phone_number_type__record_version,
    CASE
      WHEN phone_number_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE phone_number_type__record_loaded_at
    END AS phone_number_type__record_valid_from,
    COALESCE(
      LEAD(phone_number_type__record_loaded_at) OVER (PARTITION BY phone_number_type_id ORDER BY phone_number_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS phone_number_type__record_valid_to,
    phone_number_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS phone_number_type__is_current_record,
    CASE WHEN phone_number_type__is_current_record THEN phone_number_type__record_loaded_at ELSE phone_number_type__record_valid_to END AS phone_number_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('phone_number_type|adventure_works|', phone_number_type_id, '~epoch|valid_from|', phone_number_type__valid_from)::BLOB AS _pit_hook__phone_number_type,
    CONCAT('phone_number_type|adventure_works|', phone_number_type_id)::BLOB AS _hook__phone_number_type,
    *
  FROM validity
)
SELECT
  *
FROM hooks