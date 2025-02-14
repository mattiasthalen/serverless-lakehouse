MODEL (
  kind VIEW
);

SELECT
  *
  EXCLUDE (_hook__business_entity, _hook__sales_person)
FROM silver.bag__adventure_works__stores