MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    business_entity_id,
    additional_contact_info AS business_entity__additional_contact_info,
    demographics AS business_entity__demographics,
    email_promotion AS business_entity__email_promotion,
    first_name AS business_entity__first_name,
    last_name AS business_entity__last_name,
    middle_name AS business_entity__middle_name,
    modified_date AS business_entity__modified_date,
    name_style AS business_entity__name_style,
    person_type AS business_entity__person_type,
    rowguid AS business_entity__rowguid,
    suffix AS business_entity__suffix,
    title AS business_entity__title,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__persons")
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
    CONCAT('business_entity|adventure_works|', business_entity_id, '~epoch|valid_from|', business_entity__valid_from)::BLOB AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  *
FROM hooks