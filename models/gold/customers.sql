MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__customer)
FROM silver.bag__adventure_works__customers
