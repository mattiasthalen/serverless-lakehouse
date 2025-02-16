MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    address_id,
    address_type_id,
    business_entity_id,
    modified_date AS address__modified_date,
    rowguid AS address__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__business_entity_addresses")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY address__record_loaded_at) AS address__record_version,
    CASE
      WHEN address__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE address__record_loaded_at
    END AS address__record_valid_from,
    COALESCE(
      LEAD(address__record_loaded_at) OVER (PARTITION BY address_id ORDER BY address__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS address__record_valid_to,
    address__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS address__is_current_record,
    CASE WHEN address__is_current_record THEN address__record_loaded_at ELSE address__record_valid_to END AS address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('address|adventure_works|', address_id, '~epoch|valid_from|', address__valid_from)::BLOB AS _pit_hook__address,
    CONCAT('address|adventure_works|', address_id)::BLOB AS _hook__address,
    CONCAT('address_type|adventure_works|', address_type_id)::BLOB AS _hook__address_type,
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  *
FROM hooks