MODEL (
  kind VIEW
);

WITH staging AS (
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
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_order_details")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_detail_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY sales_order_detail_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order_detail|adventure_works|',
      sales_order_detail_id,
      '~epoch|valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__sales_order_detail,
    CONCAT('sales_order_detail|adventure_works|', sales_order_detail_id)::BLOB AS _hook__sales_order_detail,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('sales_order|adventure_works|', sales_order_id)::BLOB AS _hook__sales_order,
    CONCAT('special_offer|adventure_works|', special_offer_id)::BLOB AS _hook__special_offer,
    *
  FROM validity
)
SELECT
  *
FROM hooks