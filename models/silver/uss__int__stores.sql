MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    uss__int__sales_persons._pit_hook__sales_person,
    bag__adventure_works__stores._hook__store,
    bag__adventure_works__stores.store__demographics,
    bag__adventure_works__stores.store__modified_date,
    bag__adventure_works__stores.store__name,
    bag__adventure_works__stores.store__rowguid,
    GREATEST(
      bag__adventure_works__stores.store__record_loaded_at,
      uss__int__sales_persons.sales_person__record_loaded_at
    ) AS store__record_loaded_at,
    GREATEST(
      bag__adventure_works__stores.store__record_updated_at,
      uss__int__sales_persons.sales_person__record_updated_at
    ) AS store__record_updated_at,
    GREATEST(
      bag__adventure_works__stores.store__record_valid_from,
      uss__int__sales_persons.sales_person__record_valid_from
    ) AS store__record_valid_from,
    LEAST(
      bag__adventure_works__stores.store__record_valid_to,
      uss__int__sales_persons.sales_person__record_valid_to
    ) AS store__record_valid_to,
    store__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS store__is_current_record
  FROM silver.bag__adventure_works__stores
  LEFT JOIN silver.uss__int__sales_persons
    ON bag__adventure_works__stores._hook__sales_person = uss__int__sales_persons._hook__sales_person
    AND bag__adventure_works__stores.store__record_valid_from < uss__int__sales_persons.sales_person__record_valid_to
    AND bag__adventure_works__stores.store__record_valid_to > uss__int__sales_persons.sales_person__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(_hook__store::TEXT, '~epoch|valid_from', store__record_valid_from)::BLOB AS _pit_hook__store,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time