MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'ship_methods' AS stage,
    bag__adventure_works__ship_methods._pit_hook__ship_method,
    bag__adventure_works__ship_methods._hook__ship_method,
    bag__adventure_works__ship_methods.ship_method__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__ship_methods.ship_method__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__ship_methods.ship_method__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__ship_methods.ship_method__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__ship_methods
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge