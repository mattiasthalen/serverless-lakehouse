MODEL (
  kind VIEW
);

SELECT
  *
  EXCLUDE (_hook__address__bill_to, _hook__credit_card, _hook__currency_rate, _hook__customer, _hook__sales_order, _hook__sales_person, _hook__ship_method, _hook__address__ship_to, _hook__territory)
FROM silver.bag__adventure_works__sales_order_headers