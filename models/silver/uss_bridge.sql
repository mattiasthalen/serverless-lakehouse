MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    *
  FROM silver.uss_bridge__currency_rates
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__sales_order_headers
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__product_subcategories
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__sales_persons
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__products
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__state_provinces
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__sales_territories
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__sales_order_details
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__ship_methods
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__special_offers
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__product_categories
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__customers
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__addresses
  UNION ALL BY NAME
  SELECT
    *
  FROM silver.uss_bridge__credit_cards
)
SELECT
  stage,
  _pit_hook__address,
  _pit_hook__credit_card,
  _pit_hook__currency_rate,
  _pit_hook__customer,
  _pit_hook__product,
  _pit_hook__product_category,
  _pit_hook__product_subcategory,
  _pit_hook__sales_order,
  _pit_hook__sales_order_detail,
  _pit_hook__sales_person,
  _pit_hook__ship_method,
  _pit_hook__special_offer,
  _pit_hook__state_province,
  _pit_hook__territory,
  _hook__calendar__date,
  measure__sales_order_placed,
  measure__sales_order_due_lead_time,
  measure__sales_order_shipping_lead_time,
  measure__sales_order_due,
  measure__sales_order_shipped_on_time,
  measure__sales_order_shipped,
  bridge__record_loaded_at,
  bridge__record_updated_at,
  bridge__record_valid_from,
  bridge__record_valid_to,
  bridge__is_current_record
FROM bridge