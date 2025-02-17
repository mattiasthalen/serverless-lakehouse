MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    customer_id,
    person_id,
    store_id,
    territory_id,
    account_number AS customer__account_number,
    modified_date AS customer__modified_date,
    rowguid AS customer__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS customer__record_loaded_at
  FROM bronze.raw__adventure_works__customers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer__record_loaded_at) AS customer__record_version,
    CASE
      WHEN customer__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE customer__record_loaded_at
    END AS customer__record_valid_from,
    COALESCE(
      LEAD(customer__record_loaded_at) OVER (PARTITION BY customer_id ORDER BY customer__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS customer__record_valid_to,
    customer__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS customer__is_current_record,
    CASE
      WHEN customer__is_current_record
      THEN customer__record_loaded_at
      ELSE customer__record_valid_to
    END AS customer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'customer|adventure_works|',
      customer_id,
      '~epoch|valid_from|',
      customer__record_valid_from
    )::BLOB AS _pit_hook__customer,
    CONCAT('customer|adventure_works|', customer_id)::BLOB AS _hook__customer,
    CONCAT('person|adventure_works|', person_id)::BLOB AS _hook__person,
    CONCAT('store|adventure_works|', store_id)::BLOB AS _hook__store,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks