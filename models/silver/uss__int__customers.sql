MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    uss__int__stores._pit_hook__store,
    uss__int__stores._pit_hook__sales_person,
    uss__int__sales_territories._pit_hook__territory,
    bag__adventure_works__customers._hook__customer,
    bag__adventure_works__customers.customer__account_number,
    bag__adventure_works__customers.customer__modified_date,
    bag__adventure_works__customers.customer__rowguid,
    bag__adventure_works__persons.person__additional_contact_info AS customer__additional_contact_info,
    bag__adventure_works__persons.person__demographics AS customer__demographics,
    bag__adventure_works__persons.person__email_promotion AS customer__email_promotion,
    bag__adventure_works__persons.person__first_name AS customer__first_name,
    bag__adventure_works__persons.person__last_name AS customer__last_name,
    bag__adventure_works__persons.person__middle_name AS customer__middle_name,
    bag__adventure_works__persons.person__modified_date AS customer__modified_date,
    bag__adventure_works__persons.person__name_style AS customer__name_style,
    bag__adventure_works__persons.person__person_type AS customer__person_type,
    bag__adventure_works__persons.person__rowguid AS customer__rowguid,
    bag__adventure_works__persons.person__suffix AS customer__suffix,
    bag__adventure_works__persons.person__title AS customer__title,
    GREATEST(
      bag__adventure_works__customers.customer__record_loaded_at,
      bag__adventure_works__persons.person__record_loaded_at,
      uss__int__stores.store__record_loaded_at,
      uss__int__sales_territories.territory__record_loaded_at
    ) AS customer__record_loaded_at,
    GREATEST(
      bag__adventure_works__customers.customer__record_updated_at,
      bag__adventure_works__persons.person__record_updated_at,
      uss__int__stores.store__record_updated_at,
      uss__int__sales_territories.territory__record_updated_at
    ) AS customer__record_updated_at,
    GREATEST(
      bag__adventure_works__customers.customer__record_valid_from,
      bag__adventure_works__persons.person__record_valid_from,
      uss__int__stores.store__record_valid_from,
      uss__int__sales_territories.territory__record_valid_from
    ) AS customer__record_valid_from,
    LEAST(
      bag__adventure_works__customers.customer__record_valid_to,
      bag__adventure_works__persons.person__record_valid_to,
      uss__int__stores.store__record_valid_to,
      uss__int__sales_territories.territory__record_valid_to
    ) AS customer__record_valid_to,
    customer__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS customer__is_current_record
  FROM silver.bag__adventure_works__customers
  LEFT JOIN silver.bag__adventure_works__persons
    ON bag__adventure_works__customers._hook__person = bag__adventure_works__persons._hook__person
    AND bag__adventure_works__customers.customer__record_valid_from < bag__adventure_works__persons.person__record_valid_to
    AND bag__adventure_works__customers.customer__record_valid_to > bag__adventure_works__persons.person__record_valid_from
  LEFT JOIN silver.uss__int__stores
    ON bag__adventure_works__customers._hook__store = uss__int__stores._hook__store
    AND bag__adventure_works__customers.customer__record_valid_from < uss__int__stores.store__record_valid_to
    AND bag__adventure_works__customers.customer__record_valid_to > uss__int__stores.store__record_valid_from
  LEFT JOIN silver.uss__int__sales_territories
    ON bag__adventure_works__customers._hook__territory = uss__int__sales_territories._hook__territory
    AND bag__adventure_works__customers.customer__record_valid_from < uss__int__sales_territories.territory__record_valid_to
    AND bag__adventure_works__customers.customer__record_valid_to > uss__int__sales_territories.territory__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(_hook__customer::TEXT, '~epoch|valid_from', customer__record_valid_from)::BLOB AS _pit_hook__customer,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time