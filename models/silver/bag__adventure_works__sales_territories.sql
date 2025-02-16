MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    territory_id,
    cost_last_year AS territory__cost_last_year,
    cost_ytd AS territory__cost_ytd,
    country_region_code AS territory__country_region_code,
    group AS territory__group,
    modified_date AS territory__modified_date,
    name AS territory__name,
    rowguid AS territory__rowguid,
    sales_last_year AS territory__sales_last_year,
    sales_ytd AS territory__sales_ytd,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS territory__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_territories")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY territory_id ORDER BY territory__record_loaded_at) AS territory__record_version,
    CASE
      WHEN territory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE territory__record_loaded_at
    END AS territory__record_valid_from,
    COALESCE(
      LEAD(territory__record_loaded_at) OVER (PARTITION BY territory_id ORDER BY territory__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS territory__record_valid_to,
    territory__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS territory__is_current_record,
    CASE WHEN territory__is_current_record THEN territory__record_loaded_at ELSE territory__record_valid_to END AS territory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('territory|adventure_works|', territory_id, '~epoch|valid_from|', territory__valid_from)::BLOB AS _pit_hook__territory,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks