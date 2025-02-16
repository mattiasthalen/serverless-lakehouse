MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    business_entity_id,
    login_id,
    birth_date AS business_entity__birth_date,
    current_flag AS business_entity__current_flag,
    gender AS business_entity__gender,
    hire_date AS business_entity__hire_date,
    job_title AS business_entity__job_title,
    marital_status AS business_entity__marital_status,
    modified_date AS business_entity__modified_date,
    national_idnumber AS business_entity__national_idnumber,
    organization_level AS business_entity__organization_level,
    rowguid AS business_entity__rowguid,
    salaried_flag AS business_entity__salaried_flag,
    sick_leave_hours AS business_entity__sick_leave_hours,
    vacation_hours AS business_entity__vacation_hours,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__employees")
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
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    CONCAT('login|adventure_works|', login_id)::BLOB AS _hook__login,
    *
  FROM validity
)
SELECT
  *
FROM hooks