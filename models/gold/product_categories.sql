MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__product_category)
FROM silver.bag__adventure_works__product_categories
