MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    business_entity_id,
    sales_person_id,
    demographics,
    modified_date,
    name,
    rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__stores")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY business_entity_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('business_entity|adventure_works|', business_entity_id, '~epoch|valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity_id)::BLOB AS _hook__business_entity,
    CONCAT('sales_person|adventure_works|', sales_person_id)::BLOB AS _hook__sales_person,
    *
  FROM validity
)
SELECT
  *
FROM hooks