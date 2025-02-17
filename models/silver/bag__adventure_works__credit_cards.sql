MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    credit_card_id,
    card_number AS credit_card__card_number,
    card_type AS credit_card__card_type,
    exp_month AS credit_card__exp_month,
    exp_year AS credit_card__exp_year,
    modified_date AS credit_card__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS credit_card__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__credit_cards")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY credit_card_id ORDER BY credit_card__record_loaded_at) AS credit_card__record_version,
    CASE
      WHEN credit_card__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE credit_card__record_loaded_at
    END AS credit_card__record_valid_from,
    COALESCE(
      LEAD(credit_card__record_loaded_at) OVER (PARTITION BY credit_card_id ORDER BY credit_card__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS credit_card__record_valid_to,
    credit_card__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS credit_card__is_current_record,
    CASE
      WHEN credit_card__is_current_record
      THEN credit_card__record_loaded_at
      ELSE credit_card__record_valid_to
    END AS credit_card__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'credit_card|adventure_works|',
      credit_card_id,
      '~epoch|valid_from|',
      credit_card__record_valid_from
    )::BLOB AS _pit_hook__credit_card,
    CONCAT('credit_card|adventure_works|', credit_card_id)::BLOB AS _hook__credit_card,
    *
  FROM validity
)
SELECT
  *
FROM hooks