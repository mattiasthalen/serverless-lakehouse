MODEL (
  kind VIEW
);

SELECT
  *
FROM silver.uss__adventure_works__product_categories
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__sales_order_headers
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__product_subcategories
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__sales_order_details
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__products
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__ship_methods
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__currency_rates
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__persons
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__state_provinces
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__sales_territories
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__stores
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__credit_cards
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__sales_persons
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__addresses
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__customers
UNION ALL BY NAME
SELECT
  *
FROM silver.uss__adventure_works__special_offers