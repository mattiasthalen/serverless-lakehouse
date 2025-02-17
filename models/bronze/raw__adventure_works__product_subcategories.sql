MODEL (
  kind VIEW
);

SELECT
  product_subcategory_id,
  product_category_id,
  modified_date,
  name,
  rowguid
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_subcategories")