MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    address_id,
    state_province_id,
    address_line1 AS address__address_line1,
    address_line2 AS address__address_line2,
    city AS address__city,
    modified_date AS address__modified_date,
    postal_code AS address__postal_code,
    rowguid AS address__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__addresses")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY address__record_loaded_at) AS address__record_version,
    CASE
      WHEN address__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE address__record_loaded_at
    END AS address__record_valid_from,
    COALESCE(
      LEAD(address__record_loaded_at) OVER (PARTITION BY address_id ORDER BY address__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS address__record_valid_to,
    address__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS address__is_current_record,
    CASE
      WHEN address__is_current_record
      THEN address__record_loaded_at
      ELSE address__record_valid_to
    END AS address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'address|adventure_works|',
      address_id,
      '~epoch|valid_from|',
      address__record_valid_from
    )::BLOB AS _pit_hook__address,
    CONCAT('address|adventure_works|', address_id)::BLOB AS _hook__address,
    CONCAT('state_province|adventure_works|', state_province_id)::BLOB AS _hook__state_province,
    *
  FROM validity
)
SELECT
  *
FROM hooks