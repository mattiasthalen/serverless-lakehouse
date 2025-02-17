MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'addresses' AS stage,
    bag__adventure_works__addresses._pit_hook__address,
    bag__adventure_works__addresses._hook__address,
    uss_bridge__state_provinces._pit_hook__state_province,
    uss_bridge__state_provinces._pit_hook__territory,
    GREATEST(
      bag__adventure_works__addresses.address__record_loaded_at,
      uss_bridge__state_provinces.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
      bag__adventure_works__addresses.address__record_updated_at,
      uss_bridge__state_provinces.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
      bag__adventure_works__addresses.address__record_valid_from,
      uss_bridge__state_provinces.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
      bag__adventure_works__addresses.address__record_valid_to,
      uss_bridge__state_provinces.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__addresses
  LEFT JOIN silver.uss_bridge__state_provinces
    ON bag__adventure_works__addresses._hook__state_province = uss_bridge__state_provinces._hook__state_province
    AND bag__adventure_works__addresses.address__record_valid_from <= uss_bridge__state_provinces.bridge__record_valid_to
    AND bag__adventure_works__addresses.address__record_valid_to >= uss_bridge__state_provinces.bridge__record_valid_from
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge