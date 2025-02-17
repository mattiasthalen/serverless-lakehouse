MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__sales_person)
FROM silver.bag__adventure_works__sales_persons
