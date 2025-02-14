MODEL (
  kind VIEW
);

SELECT
  'adventure_works__products' AS stage,
  _pit_hook__product,
  _hook__product,
  _hook__product_model,
  _hook__product_subcategory,
  _sqlmesh__loaded_at,
  _sqlmesh__version,
  _sqlmesh__valid_from,
  _sqlmesh__valid_to,
  _sqlmesh__is_current,
  _sqlmesh__updated_at
FROM silver.bag__adventure_works__products