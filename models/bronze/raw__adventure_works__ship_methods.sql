MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  ship_method_id,
  modified_date,
  name,
  rowguid,
  ship_base,
  ship_rate,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__ship_methods")