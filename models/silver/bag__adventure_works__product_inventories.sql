MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    location_id,
    product_id,
    bin AS location__bin,
    modified_date AS location__modified_date,
    quantity AS location__quantity,
    rowguid AS location__rowguid,
    shelf AS location__shelf,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS location__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_inventories")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY location__record_loaded_at) AS location__record_version,
    CASE
      WHEN location__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE location__record_loaded_at
    END AS location__record_valid_from,
    COALESCE(
      LEAD(location__record_loaded_at) OVER (PARTITION BY location_id ORDER BY location__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS location__record_valid_to,
    location__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS location__is_current_record,
    CASE WHEN location__is_current_record THEN location__record_loaded_at ELSE location__record_valid_to END AS location__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('location|adventure_works|', location_id, '~epoch|valid_from|', location__record_valid_from)::BLOB AS _pit_hook__location,
    CONCAT('location|adventure_works|', location_id)::BLOB AS _hook__location,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    *
  FROM validity
)
SELECT
  *
FROM hooks