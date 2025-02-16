MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    job_candidate_id,
    business_entity_id,
    modified_date AS job_candidate__modified_date,
    resume AS job_candidate__resume,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS job_candidate__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__job_candidates")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY job_candidate_id ORDER BY job_candidate__record_loaded_at) AS job_candidate__record_version,
    CASE
      WHEN job_candidate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE job_candidate__record_loaded_at
    END AS job_candidate__record_valid_from,
    COALESCE(
      LEAD(job_candidate__record_loaded_at) OVER (PARTITION BY job_candidate_id ORDER BY job_candidate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS job_candidate__record_valid_to,
    job_candidate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS job_candidate__is_current_record,
    CASE WHEN job_candidate__is_current_record THEN job_candidate__record_loaded_at ELSE job_candidate__record_valid_to END AS job_candidate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('job_candidate|adventure_works|', job_candidate_id, '~epoch|valid_from|', job_candidate__record_valid_from)::BLOB AS _pit_hook__job_candidate,
    CONCAT('job_candidate|adventure_works|', job_candidate_id)::BLOB AS _hook__job_candidate,
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  *
FROM hooks