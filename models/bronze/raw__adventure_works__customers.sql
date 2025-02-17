MODEL (
  kind VIEW
);

SELECT
  customer_id,
  person_id,
  store_id,
  territory_id,
  account_number,
  modified_date,
  rowguid
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__customers")