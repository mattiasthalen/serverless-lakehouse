MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    business_entity_id AS sales_person_id,
    territory_id,
    bonus AS sales_person__bonus,
    commission_pct AS sales_person__commission_pct,
    modified_date AS sales_person__modified_date,
    rowguid AS sales_person__rowguid,
    sales_last_year AS sales_person__sales_last_year,
    sales_quota AS sales_person__sales_quota,
    sales_ytd AS sales_person__sales_ytd,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_person__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_persons")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_person_id ORDER BY sales_person__record_loaded_at) AS sales_person__record_version,
    CASE
      WHEN sales_person__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_person__record_loaded_at
    END AS sales_person__record_valid_from,
    COALESCE(
      LEAD(sales_person__record_loaded_at) OVER (PARTITION BY sales_person_id ORDER BY sales_person__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_person__record_valid_to,
    sales_person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_person__is_current_record,
    CASE WHEN sales_person__is_current_record THEN sales_person__record_loaded_at ELSE sales_person__record_valid_to END AS sales_person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('sales_person|adventure_works|', sales_person_id, '~epoch|valid_from|', sales_person__record_valid_from)::BLOB AS _pit_hook__sales_person,
    CONCAT('sales_person|adventure_works|', sales_person_id)::BLOB AS _hook__sales_person,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks