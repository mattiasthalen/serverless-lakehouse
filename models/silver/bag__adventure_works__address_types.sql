MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    address_type_id,
    modified_date AS address_type__modified_date,
    name AS address_type__name,
    rowguid AS address_type__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address_type__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__address_types")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_type_id ORDER BY address_type__record_loaded_at) AS address_type__record_version,
    CASE
      WHEN address_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE address_type__record_loaded_at
    END AS address_type__record_valid_from,
    COALESCE(
      LEAD(address_type__record_loaded_at) OVER (PARTITION BY address_type_id ORDER BY address_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS address_type__record_valid_to,
    address_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS address_type__is_current_record,
    CASE WHEN address_type__is_current_record THEN address_type__record_loaded_at ELSE address_type__record_valid_to END AS address_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('address_type|adventure_works|', address_type_id)::BLOB AS _hook__address_type,
    *
  FROM validity
)
SELECT
  *
FROM hooks