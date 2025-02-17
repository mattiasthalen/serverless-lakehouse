MODEL (
  kind FULL
);

SELECT
  credit_card_id,
  card_number,
  card_type,
  exp_month,
  exp_year,
  modified_date,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__credit_cards")