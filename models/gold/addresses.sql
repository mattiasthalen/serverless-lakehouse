MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__address, _hook__state_province, _hook__territory)
FROM silver.bag__adventure_works__addresses
