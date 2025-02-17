MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__product, _hook__product_model, _hook__product_subcategory)
FROM silver.bag__adventure_works__products
