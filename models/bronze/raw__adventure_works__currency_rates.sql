MODEL (
  kind VIEW
);

SELECT
  currency_rate_id,
  average_rate,
  currency_rate_date,
  end_of_day_rate,
  from_currency_code,
  modified_date,
  to_currency_code,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__currency_rates")