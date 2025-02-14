MODEL (
  kind VIEW
);

SELECT
  'adventure_works__persons' AS stage,
  _pit_hook__business_entity,
  _hook__business_entity,
  _sqlmesh__loaded_at,
  _sqlmesh__version,
  _sqlmesh__valid_from,
  _sqlmesh__valid_to,
  _sqlmesh__is_current,
  _sqlmesh__updated_at
FROM silver.bag__adventure_works__persons