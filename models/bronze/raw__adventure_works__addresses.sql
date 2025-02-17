MODEL (
  kind VIEW
);

SELECT
  address_id,
  state_province_id,
  address_line1,
  address_line2,
  city,
  modified_date,
  postal_code,
  rowguid
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__addresses")