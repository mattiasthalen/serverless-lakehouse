MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'currency_rates' AS stage,
    bag__adventure_works__currency_rates._pit_hook__currency_rate,
    bag__adventure_works__currency_rates._hook__currency_rate,
    bag__adventure_works__currency_rates.currency_rate__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__currency_rates.currency_rate__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__currency_rates.currency_rate__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__currency_rates.currency_rate__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__currency_rates
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge