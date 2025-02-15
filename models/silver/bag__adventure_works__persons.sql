MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    business_entity_id,
    additional_contact_info AS person__additional_contact_info,
    demographics AS person__demographics,
    email_promotion AS person__email_promotion,
    first_name AS person__first_name,
    last_name AS person__last_name,
    middle_name AS person__middle_name,
    modified_date AS person__modified_date,
    name_style AS person__name_style,
    person_type AS person__person_type,
    rowguid AS person__rowguid,
    suffix AS person__suffix,
    title AS person__title,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__persons")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_id ORDER BY person__record_loaded_at) AS person__record_version,
    CASE
      WHEN person__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE person__record_loaded_at
    END AS person__record_valid_from,
    COALESCE(
      LEAD(person__record_loaded_at) OVER (PARTITION BY business_entity_id ORDER BY person__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS person__record_valid_to,
    person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS person__is_current_record,
    CASE
      WHEN person__is_current_record
      THEN person__record_loaded_at
      ELSE person__record_valid_to
    END AS person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person|adventure_works|', business_entity_id)::BLOB AS _hook__person,
    *
  FROM validity
)
SELECT
  *
FROM hooks