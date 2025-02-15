MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    ship_method_id,
    modified_date AS ship_method__modified_date,
    name AS ship_method__name,
    rowguid AS ship_method__rowguid,
    ship_base AS ship_method__ship_base,
    ship_rate AS ship_method__ship_rate,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS ship_method__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__ship_methods")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ship_method_id ORDER BY ship_method__record_loaded_at) AS ship_method__record_version,
    CASE
      WHEN ship_method__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE ship_method__record_loaded_at
    END AS ship_method__record_valid_from,
    COALESCE(
      LEAD(ship_method__record_loaded_at) OVER (PARTITION BY ship_method_id ORDER BY ship_method__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS ship_method__record_valid_to,
    ship_method__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS ship_method__is_current_record,
    CASE
      WHEN ship_method__is_current_record
      THEN ship_method__record_loaded_at
      ELSE ship_method__record_valid_to
    END AS ship_method__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('ship_method|adventure_works|', ship_method_id)::BLOB AS _hook__ship_method,
    *
  FROM validity
)
SELECT
  *
FROM hooks