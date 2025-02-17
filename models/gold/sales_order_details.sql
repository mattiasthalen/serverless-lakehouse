MODEL (
  kind VIEW
);

SELECT
  * EXCLUDE(_hook__product, _hook__sales_order, _hook__sales_order_detail, _hook__special_offer)
FROM silver.bag__adventure_works__sales_order_details
