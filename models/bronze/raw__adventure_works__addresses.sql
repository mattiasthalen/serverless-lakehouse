MODEL (
  kind FULL
);

SELECT
  address_id,
  state_province_id,
  address_line1,
  address_line2,
  city,
  modified_date,
  postal_code,
  rowguid,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__addresses")