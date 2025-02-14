MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    address_id,
    state_province_id,
    address_line1,
    address_line2,
    city,
    modified_date,
    postal_code,
    rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__addresses")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY address_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('address|adventure_works|', address_id, '~epoch|valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__address,
    CONCAT('address|adventure_works|', address_id)::BLOB AS _hook__address,
    CONCAT('state_province|adventure_works|', state_province_id)::BLOB AS _hook__state_province,
    *
  FROM validity
)
SELECT
  *
FROM hooks