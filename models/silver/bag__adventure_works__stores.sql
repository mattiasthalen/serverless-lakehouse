MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    business_entity_id,
    sales_person_id,
    demographics AS store__demographics,
    modified_date AS store__modified_date,
    name AS store__name,
    rowguid AS store__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS store__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__stores")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_id ORDER BY store__record_loaded_at) AS store__record_version,
    CASE
      WHEN store__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE store__record_loaded_at
    END AS store__record_valid_from,
    COALESCE(
      LEAD(store__record_loaded_at) OVER (PARTITION BY business_entity_id ORDER BY store__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS store__record_valid_to,
    store__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS store__is_current_record,
    CASE
      WHEN store__is_current_record
      THEN store__record_loaded_at
      ELSE store__record_valid_to
    END AS store__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('store|adventure_works|', business_entity_id)::BLOB AS _hook__store,
    CONCAT('sales_person|adventure_works|', sales_person_id)::BLOB AS _hook__sales_person,
    *
  FROM validity
)
SELECT
  *
FROM hooks