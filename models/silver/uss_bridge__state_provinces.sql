MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'state_provinces' AS stage,
    bag__adventure_works__state_provinces._pit_hook__state_province,
    bag__adventure_works__state_provinces._hook__state_province,
    uss_bridge__sales_territories._pit_hook__territory,
    GREATEST(
      bag__adventure_works__state_provinces.state_province__record_loaded_at,
      uss_bridge__sales_territories.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
      bag__adventure_works__state_provinces.state_province__record_updated_at,
      uss_bridge__sales_territories.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
      bag__adventure_works__state_provinces.state_province__record_valid_from,
      uss_bridge__sales_territories.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
      bag__adventure_works__state_provinces.state_province__record_valid_to,
      uss_bridge__sales_territories.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__state_provinces
  LEFT JOIN silver.uss_bridge__sales_territories
    ON bag__adventure_works__state_provinces._hook__territory = uss_bridge__sales_territories._hook__territory
    AND bag__adventure_works__state_provinces.state_province__record_valid_from <= uss_bridge__sales_territories.bridge__record_valid_to
    AND bag__adventure_works__state_provinces.state_province__record_valid_to >= uss_bridge__sales_territories.bridge__record_valid_from
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge