MODEL (
  kind VIEW
);

SELECT
  transaction_id,
  product_id,
  reference_order_id,
  reference_order_line_id,
  transaction_date,
  transaction_type,
  quantity,
  actual_cost,
  modified_date,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__transaction_histories")