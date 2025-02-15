MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    bag__adventure_works__sales_territories._hook__territory,
    bag__adventure_works__sales_territories.territory__cost_last_year,
    bag__adventure_works__sales_territories.territory__cost_ytd,
    bag__adventure_works__sales_territories.territory__country_region_code,
    bag__adventure_works__sales_territories.territory__group,
    bag__adventure_works__sales_territories.territory__modified_date,
    bag__adventure_works__sales_territories.territory__name,
    bag__adventure_works__sales_territories.territory__rowguid,
    bag__adventure_works__sales_territories.territory__sales_last_year,
    bag__adventure_works__sales_territories.territory__sales_ytd,
    bag__adventure_works__sales_territories.territory__record_loaded_at,
    bag__adventure_works__sales_territories.territory__record_updated_at,
    bag__adventure_works__sales_territories.territory__record_valid_from,
    bag__adventure_works__sales_territories.territory__record_valid_to,
    bag__adventure_works__sales_territories.territory__is_current_record
  FROM silver.bag__adventure_works__sales_territories
), point_in_time AS (
  SELECT
    CONCAT(_hook__territory::TEXT, '~epoch|valid_from', territory__record_valid_from)::BLOB AS _pit_hook__territory,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time