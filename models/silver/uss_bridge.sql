MODEL (
  kind VIEW
);

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