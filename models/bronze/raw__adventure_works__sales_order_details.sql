MODEL (
  kind FULL
);

SELECT
  sales_order_detail_id,
  product_id,
  sales_order_id,
  special_offer_id,
  carrier_tracking_number,
  line_total,
  modified_date,
  order_qty,
  rowguid,
  unit_price,
  unit_price_discount,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_order_details")