MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    business_entity_id,
    email_address_id,
    email AS business_entity__email,
    modified_date AS business_entity__modified_date,
    rowguid AS business_entity__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__email_addresses")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_id ORDER BY business_entity__record_loaded_at) AS business_entity__record_version,
    CASE
      WHEN business_entity__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE business_entity__record_loaded_at
    END AS business_entity__record_valid_from,
    COALESCE(
      LEAD(business_entity__record_loaded_at) OVER (PARTITION BY business_entity_id ORDER BY business_entity__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS business_entity__record_valid_to,
    business_entity__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS business_entity__is_current_record,
    CASE WHEN business_entity__is_current_record THEN business_entity__record_loaded_at ELSE business_entity__record_valid_to END AS business_entity__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('business_entity|adventure_works|', business_entity_id, '~epoch|valid_from|', business_entity__record_valid_from)::BLOB AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    CONCAT('email_address|adventure_works|', email_address_id)::BLOB AS _hook__email_address,
    *
  FROM validity
)
SELECT
  *
FROM hooks