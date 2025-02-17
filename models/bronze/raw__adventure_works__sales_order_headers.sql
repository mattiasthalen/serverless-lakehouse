MODEL (
  kind FULL
);

SELECT
  sales_order_id,
  bill_to_address_id,
  credit_card_id,
  currency_rate_id,
  customer_id,
  sales_person_id,
  ship_method_id,
  ship_to_address_id,
  territory_id,
  account_number,
  credit_card_approval_code,
  due_date,
  freight,
  modified_date,
  online_order_flag,
  order_date,
  purchase_order_number,
  revision_number,
  rowguid,
  sales_order_number,
  ship_date,
  status,
  sub_total,
  tax_amt,
  total_due,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_order_headers")