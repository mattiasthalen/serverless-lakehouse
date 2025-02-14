MODEL (
  kind VIEW
);

SELECT
    _pit_hook__currency_rate,
    _hook__currency_rate,    
    _sqlmesh__loaded_at,
    _sqlmesh__version,
    _sqlmesh__valid_from,
    _sqlmesh__valid_to,
    _sqlmesh__is_current,
    _sqlmesh__updated_at
FROM silver.bag__adventure_works__currency_rates
