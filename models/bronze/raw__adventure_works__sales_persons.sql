MODEL (
  kind FULL
);

SELECT
  business_entity_id,
  territory_id,
  bonus,
  commission_pct,
  modified_date,
  rowguid,
  sales_last_year,
  sales_quota,
  sales_ytd,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_persons")