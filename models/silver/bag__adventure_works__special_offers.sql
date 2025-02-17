MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    special_offer_id,
    category AS special_offer__category,
    description AS special_offer__description,
    discount_percentage AS special_offer__discount_percentage,
    end_date AS special_offer__end_date,
    maximum_quantity AS special_offer__maximum_quantity,
    minimum_quantity AS special_offer__minimum_quantity,
    modified_date AS special_offer__modified_date,
    rowguid AS special_offer__rowguid,
    start_date AS special_offer__start_date,
    type AS special_offer__type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS special_offer__record_loaded_at
  FROM bronze.raw__adventure_works__special_offers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY special_offer_id ORDER BY special_offer__record_loaded_at) AS special_offer__record_version,
    CASE
      WHEN special_offer__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE special_offer__record_loaded_at
    END AS special_offer__record_valid_from,
    COALESCE(
      LEAD(special_offer__record_loaded_at) OVER (PARTITION BY special_offer_id ORDER BY special_offer__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS special_offer__record_valid_to,
    special_offer__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS special_offer__is_current_record,
    CASE
      WHEN special_offer__is_current_record
      THEN special_offer__record_loaded_at
      ELSE special_offer__record_valid_to
    END AS special_offer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'special_offer|adventure_works|',
      special_offer_id,
      '~epoch|valid_from|',
      special_offer__record_valid_from
    )::BLOB AS _pit_hook__special_offer,
    CONCAT('special_offer|adventure_works|', special_offer_id)::BLOB AS _hook__special_offer,
    *
  FROM validity
)
SELECT
  *
FROM hooks