MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__address, _hook__credit_card, _hook__currency_rate, _hook__customer, _hook__product, _hook__product_category, _hook__product_subcategory, _hook__sales_order, _hook__sales_order_detail, _hook__sales_person, _hook__ship_method, _hook__special_offer, _hook__state_province, _hook__territory)
FROM silver.bag__adventure_works__sales_order_details
