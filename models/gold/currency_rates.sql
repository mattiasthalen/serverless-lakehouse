MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__currency_rate)
FROM silver.bag__adventure_works__currency_rates
