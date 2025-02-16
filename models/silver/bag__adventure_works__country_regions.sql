MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    country_region_code AS country_region_code__country_region_code,
    modified_date AS country_region_code__modified_date,
    name AS country_region_code__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS country_region_code__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__country_regions")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_region_code ORDER BY country_region_code__record_loaded_at) AS country_region_code__record_version,
    CASE
      WHEN country_region_code__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE country_region_code__record_loaded_at
    END AS country_region_code__record_valid_from,
    COALESCE(
      LEAD(country_region_code__record_loaded_at) OVER (PARTITION BY country_region_code ORDER BY country_region_code__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS country_region_code__record_valid_to,
    country_region_code__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS country_region_code__is_current_record,
    CASE WHEN country_region_code__is_current_record THEN country_region_code__record_loaded_at ELSE country_region_code__record_valid_to END AS country_region_code__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('country_region_code|adventure_works|', country_region_code)::BLOB AS _hook__country_region_code,
    *
  FROM validity
)
SELECT
  *
FROM hooks