MODEL (
  kind VIEW
);

SELECT
  *
  EXCLUDE (_hook__business_entity)
FROM silver.bag__adventure_works__persons