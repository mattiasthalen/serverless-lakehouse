MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'sales_order_headers' AS stage,
    bag__adventure_works__sales_order_headers._pit_hook__sales_order,
    bag__adventure_works__sales_order_headers._hook__sales_order,
    uss_bridge__credit_cards._pit_hook__credit_card,
    uss_bridge__currency_rates._pit_hook__currency_rate,
    uss_bridge__customers._pit_hook__customer,
    uss_bridge__sales_persons._pit_hook__sales_person,
    GREATEST(
        bag__adventure_works__sales_order_headers.sales_order__record_loaded_at,
        uss_bridge__credit_cards.bridge__record_loaded_at,
        uss_bridge__currency_rates.bridge__record_loaded_at,
        uss_bridge__customers.bridge__record_loaded_at,
        uss_bridge__sales_persons.bridge__record_loaded_at
       ) AS bridge__record_loaded_at,
    GREATEST(
        bag__adventure_works__sales_order_headers.sales_order__record_updated_at,
        uss_bridge__credit_cards.bridge__record_updated_at,
        uss_bridge__currency_rates.bridge__record_updated_at,
        uss_bridge__customers.bridge__record_updated_at,
        uss_bridge__sales_persons.bridge__record_updated_at
       ) AS bridge__record_updated_at,
    GREATEST(
        bag__adventure_works__sales_order_headers.sales_order__record_valid_from,
        uss_bridge__credit_cards.bridge__record_valid_from,
        uss_bridge__currency_rates.bridge__record_valid_from,
        uss_bridge__customers.bridge__record_valid_from,
        uss_bridge__sales_persons.bridge__record_valid_from
       ) AS bridge__record_valid_from,
    LEAST(
        bag__adventure_works__sales_order_headers.sales_order__record_valid_to,
        uss_bridge__credit_cards.bridge__record_valid_to,
        uss_bridge__currency_rates.bridge__record_valid_to,
        uss_bridge__customers.bridge__record_valid_to,
        uss_bridge__sales_persons.bridge__record_valid_to
       ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__sales_order_headers
  LEFT JOIN silver.uss_bridge__credit_cards
    ON bag__adventure_works__sales_order_headers._hook__credit_card = uss_bridge__credit_cards._hook__credit_card
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from <= uss_bridge__credit_cards.bridge__record_valid_to
  LEFT JOIN silver.uss_bridge__currency_rates
    ON bag__adventure_works__sales_order_headers._hook__currency_rate = uss_bridge__currency_rates._hook__currency_rate
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from <= uss_bridge__currency_rates.bridge__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to >= uss_bridge__currency_rates.bridge__record_valid_from
  LEFT JOIN silver.uss_bridge__customers
    ON bag__adventure_works__sales_order_headers._hook__customer = uss_bridge__customers._hook__customer
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from <= uss_bridge__customers.bridge__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to >= uss_bridge__customers.bridge__record_valid_from
  LEFT JOIN silver.uss_bridge__sales_persons
    ON bag__adventure_works__sales_order_headers._hook__sales_person = uss_bridge__sales_persons._hook__sales_person
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from <= uss_bridge__sales_persons.bridge__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to >= uss_bridge__sales_persons.bridge__record_valid_from

    /*
_hook__bill_to_address,
_hook__ship_method,
_hook__ship_to_address,
_hook__territory,
    */
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge