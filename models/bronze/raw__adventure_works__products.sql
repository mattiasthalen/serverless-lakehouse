MODEL (
  kind FULL
);

SELECT
  product_id,
  product_model_id,
  product_subcategory_id,
  class,
  color,
  days_to_manufacture,
  finished_goods_flag,
  list_price,
  make_flag,
  modified_date,
  name,
  product_line,
  product_number,
  reorder_point,
  rowguid,
  safety_stock_level,
  sell_end_date,
  sell_start_date,
  size,
  size_unit_measure_code,
  standard_cost,
  style,
  weight,
  weight_unit_measure_code,
  _dlt_load_id
FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__products")