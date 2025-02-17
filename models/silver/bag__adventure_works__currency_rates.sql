MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    currency_rate_id,
    average_rate AS currency_rate__average_rate,
    currency_rate_date AS currency_rate__currency_rate_date,
    end_of_day_rate AS currency_rate__end_of_day_rate,
    from_currency_code AS currency_rate__from_currency_code,
    modified_date AS currency_rate__modified_date,
    to_currency_code AS currency_rate__to_currency_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency_rate__record_loaded_at
  FROM bronze.raw__adventure_works__currency_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_rate_id ORDER BY currency_rate__record_loaded_at) AS currency_rate__record_version,
    CASE
      WHEN currency_rate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE currency_rate__record_loaded_at
    END AS currency_rate__record_valid_from,
    COALESCE(
      LEAD(currency_rate__record_loaded_at) OVER (PARTITION BY currency_rate_id ORDER BY currency_rate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS currency_rate__record_valid_to,
    currency_rate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS currency_rate__is_current_record,
    CASE
      WHEN currency_rate__is_current_record
      THEN currency_rate__record_loaded_at
      ELSE currency_rate__record_valid_to
    END AS currency_rate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency_rate|adventure_works|',
      currency_rate_id,
      '~epoch|valid_from|',
      currency_rate__record_valid_from
    )::BLOB AS _pit_hook__currency_rate,
    CONCAT('currency_rate|adventure_works|', currency_rate_id)::BLOB AS _hook__currency_rate,
    *
  FROM validity
)
SELECT
  *
FROM hooks