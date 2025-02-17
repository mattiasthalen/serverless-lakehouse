MODEL (
  kind FULL
);

SELECT
  product_category_id,
  modified_date,
  name,
  rowguid,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_categories")