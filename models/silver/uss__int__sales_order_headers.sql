MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    uss__int__customers._pit_hook__customer,
    uss__int__customers._pit_hook__store,
    COALESCE(
      uss__int__sales_persons._pit_hook__sales_person,
      uss__int__customers._pit_hook__sales_person
    ) AS _pit_hook__sales_person,
    COALESCE(
      uss__int__sales_territories._pit_hook__territory,
      uss__int__customers._pit_hook__territory
    ) AS _pit_hook__territory,
    bag__adventure_works__sales_order_headers._hook__sales_order,
    bag__adventure_works__sales_order_headers.sales_order__account_number,
    bag__adventure_works__sales_order_headers.sales_order__credit_card_approval_code,
    bag__adventure_works__sales_order_headers.sales_order__due_date,
    bag__adventure_works__sales_order_headers.sales_order__freight,
    bag__adventure_works__sales_order_headers.sales_order__modified_date,
    bag__adventure_works__sales_order_headers.sales_order__online_order_flag,
    bag__adventure_works__sales_order_headers.sales_order__order_date,
    bag__adventure_works__sales_order_headers.sales_order__purchase_order_number,
    bag__adventure_works__sales_order_headers.sales_order__revision_number,
    bag__adventure_works__sales_order_headers.sales_order__rowguid,
    bag__adventure_works__sales_order_headers.sales_order__sales_order_number,
    bag__adventure_works__sales_order_headers.sales_order__ship_date,
    bag__adventure_works__sales_order_headers.sales_order__status,
    bag__adventure_works__sales_order_headers.sales_order__sub_total,
    bag__adventure_works__sales_order_headers.sales_order__tax_amt,
    bag__adventure_works__sales_order_headers.sales_order__total_due,
    bill_to_address.address__address_line1 AS bill_to_address__address_line1,
    bill_to_address.address__address_line2 AS bill_to_address__address_line2,
    bill_to_address.address__city AS bill_to_address__city,
    bill_to_address.address__modified_date AS bill_to_address__modified_date,
    bill_to_address.address__postal_code AS bill_to_address__postal_code,
    bill_to_address.address__rowguid AS bill_to_address__rowguid,
    ship_to_address.address__address_line1 AS ship_to_address__address_line1,
    ship_to_address.address__address_line2 AS ship_to_address__address_line2,
    ship_to_address.address__city AS ship_to_address__city,
    ship_to_address.address__modified_date AS ship_to_address__modified_date,
    ship_to_address.address__postal_code AS ship_to_address__postal_code,
    ship_to_address.address__rowguid AS ship_to_address__rowguid,
    bag__adventure_works__ship_methods.ship_method__modified_date,
    bag__adventure_works__ship_methods.ship_method__name,
    bag__adventure_works__ship_methods.ship_method__rowguid,
    bag__adventure_works__ship_methods.ship_method__ship_base,
    bag__adventure_works__ship_methods.ship_method__ship_rate,
    bag__adventure_works__credit_cards.credit_card__card_number,
    bag__adventure_works__credit_cards.credit_card__card_type,
    bag__adventure_works__credit_cards.credit_card__exp_month,
    bag__adventure_works__credit_cards.credit_card__exp_year,
    bag__adventure_works__credit_cards.credit_card__modified_date,
    bag__adventure_works__currency_rates.currency_rate__average_rate,
    bag__adventure_works__currency_rates.currency_rate__currency_rate_date,
    bag__adventure_works__currency_rates.currency_rate__end_of_day_rate,
    bag__adventure_works__currency_rates.currency_rate__from_currency_code,
    bag__adventure_works__currency_rates.currency_rate__modified_date,
    bag__adventure_works__currency_rates.currency_rate__to_currency_code,
    GREATEST(
      bag__adventure_works__sales_order_headers.sales_order__record_loaded_at,
      uss__int__customers.customer__record_loaded_at,
      uss__int__customers.customer__record_loaded_at,
      uss__int__sales_persons.sales_person__record_loaded_at,
      uss__int__sales_territories.territory__record_loaded_at,
      bill_to_address.address__record_loaded_at,
      ship_to_address.address__record_loaded_at,
      bag__adventure_works__ship_methods.ship_method__record_loaded_at,
      bag__adventure_works__credit_cards.credit_card__record_loaded_at,
      bag__adventure_works__currency_rates.currency_rate__record_loaded_at
    ) AS sales_order__record_loaded_at,
    GREATEST(
      bag__adventure_works__sales_order_headers.sales_order__record_updated_at,
      uss__int__customers.customer__record_updated_at,
      uss__int__customers.customer__record_updated_at,
      uss__int__sales_persons.sales_person__record_updated_at,
      uss__int__sales_territories.territory__record_updated_at,
      bill_to_address.address__record_updated_at,
      ship_to_address.address__record_updated_at,
      bag__adventure_works__ship_methods.ship_method__record_updated_at,
      bag__adventure_works__credit_cards.credit_card__record_updated_at,
      bag__adventure_works__currency_rates.currency_rate__record_updated_at
    ) AS sales_order__record_updated_at,
    GREATEST(
      bag__adventure_works__sales_order_headers.sales_order__record_valid_from,
      uss__int__customers.customer__record_valid_from,
      uss__int__customers.customer__record_valid_from,
      uss__int__sales_persons.sales_person__record_valid_from,
      uss__int__sales_territories.territory__record_valid_from,
      bill_to_address.address__record_valid_from,
      ship_to_address.address__record_valid_from,
      bag__adventure_works__ship_methods.ship_method__record_valid_from,
      bag__adventure_works__credit_cards.credit_card__record_valid_from,
      bag__adventure_works__currency_rates.currency_rate__record_valid_from
    ) AS sales_order__record_valid_from,
    LEAST(
      bag__adventure_works__sales_order_headers.sales_order__record_valid_to,
      uss__int__customers.customer__record_valid_to,
      uss__int__customers.customer__record_valid_to,
      uss__int__sales_persons.sales_person__record_valid_to,
      uss__int__sales_territories.territory__record_valid_to,
      bill_to_address.address__record_valid_to,
      ship_to_address.address__record_valid_to,
      bag__adventure_works__ship_methods.ship_method__record_valid_to,
      bag__adventure_works__credit_cards.credit_card__record_valid_to,
      bag__adventure_works__currency_rates.currency_rate__record_valid_to
    ) AS sales_order__record_valid_to,
    sales_order__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_order__is_current_record
  FROM silver.bag__adventure_works__sales_order_headers
  LEFT JOIN silver.uss__int__customers
    ON bag__adventure_works__sales_order_headers._hook__customer = uss__int__customers._hook__customer
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < uss__int__customers.customer__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > uss__int__customers.customer__record_valid_from
  LEFT JOIN silver.uss__int__sales_persons
    ON bag__adventure_works__sales_order_headers._hook__sales_person = uss__int__sales_persons._hook__sales_person
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < uss__int__sales_persons.sales_person__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > uss__int__sales_persons.sales_person__record_valid_from
  LEFT JOIN silver.uss__int__sales_territories
    ON bag__adventure_works__sales_order_headers._hook__territory = uss__int__sales_territories._hook__territory
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < uss__int__sales_territories.territory__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > uss__int__sales_territories.territory__record_valid_from
  LEFT JOIN silver.bag__adventure_works__addresses AS bill_to_address
    ON bag__adventure_works__sales_order_headers._hook__bill_to_address = bill_to_address._hook__address
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < bill_to_address.address__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > bill_to_address.address__record_valid_from
  LEFT JOIN silver.bag__adventure_works__addresses AS ship_to_address
    ON bag__adventure_works__sales_order_headers._hook__ship_to_address = ship_to_address._hook__address
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < ship_to_address.address__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > ship_to_address.address__record_valid_from
  LEFT JOIN silver.bag__adventure_works__ship_methods
    ON bag__adventure_works__sales_order_headers._hook__ship_method = bag__adventure_works__ship_methods._hook__ship_method
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < bag__adventure_works__ship_methods.ship_method__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > bag__adventure_works__ship_methods.ship_method__record_valid_from
  LEFT JOIN silver.bag__adventure_works__credit_cards
    ON bag__adventure_works__sales_order_headers._hook__credit_card = bag__adventure_works__credit_cards._hook__credit_card
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < bag__adventure_works__credit_cards.credit_card__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > bag__adventure_works__credit_cards.credit_card__record_valid_from
  LEFT JOIN silver.bag__adventure_works__currency_rates
    ON bag__adventure_works__sales_order_headers._hook__currency_rate = bag__adventure_works__currency_rates._hook__currency_rate
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_from < bag__adventure_works__currency_rates.currency_rate__record_valid_to
    AND bag__adventure_works__sales_order_headers.sales_order__record_valid_to > bag__adventure_works__currency_rates.currency_rate__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(_hook__sales_order::TEXT, '~epoch|valid_from', sales_order__record_valid_from)::BLOB AS _pit_hook__sales_order,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time