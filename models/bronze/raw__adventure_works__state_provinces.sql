MODEL (
  kind VIEW
);

SELECT
  state_province_id,
  territory_id,
  country_region_code,
  is_only_state_province_flag,
  modified_date,
  name,
  rowguid,
  state_province_code
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__state_provinces")