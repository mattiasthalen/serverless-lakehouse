MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__product_category, _hook__product_subcategory)
FROM silver.bag__adventure_works__product_subcategories
