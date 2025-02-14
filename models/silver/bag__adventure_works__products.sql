MODEL (
  kind VIEW
);

WITH staging AS (
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
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__products")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY product_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product|adventure_works|', product_id, '~epoch|valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__product,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('product_model|adventure_works|', product_model_id)::BLOB AS _hook__product_model,
    CONCAT('product_subcategory|adventure_works|', product_subcategory_id)::BLOB AS _hook__product_subcategory,
    *
  FROM validity
)
SELECT
  *
FROM hooks