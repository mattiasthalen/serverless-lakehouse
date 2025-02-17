MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__special_offer)
FROM silver.bag__adventure_works__special_offers
