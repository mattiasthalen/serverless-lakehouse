MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    currency_rate_id,
    average_rate,
    currency_rate_date,
    end_of_day_rate,
    from_currency_code,
    modified_date,
    to_currency_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__currency_rates")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_rate_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY currency_rate_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency_rate|adventure_works|',
      currency_rate_id,
      '~epoch|valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__currency_rate,
    CONCAT('currency_rate|adventure_works|', currency_rate_id)::BLOB AS _hook__currency_rate,
    *
  FROM validity
)
SELECT
  *
FROM hooks