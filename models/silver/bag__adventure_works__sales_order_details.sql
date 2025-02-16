MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    sales_order_detail_id,
    product_id,
    sales_order_id,
    special_offer_id,
    carrier_tracking_number AS sales_order_detail__carrier_tracking_number,
    line_total AS sales_order_detail__line_total,
    modified_date AS sales_order_detail__modified_date,
    order_qty AS sales_order_detail__order_qty,
    rowguid AS sales_order_detail__rowguid,
    unit_price AS sales_order_detail__unit_price,
    unit_price_discount AS sales_order_detail__unit_price_discount,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order_detail__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_order_details")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at) AS sales_order_detail__record_version,
    CASE
      WHEN sales_order_detail__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_order_detail__record_loaded_at
    END AS sales_order_detail__record_valid_from,
    COALESCE(
      LEAD(sales_order_detail__record_loaded_at) OVER (PARTITION BY sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_order_detail__record_valid_to,
    sales_order_detail__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_order_detail__is_current_record,
    CASE WHEN sales_order_detail__is_current_record THEN sales_order_detail__record_loaded_at ELSE sales_order_detail__record_valid_to END AS sales_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('sales_order_detail|adventure_works|', sales_order_detail_id, '~epoch|valid_from|', sales_order_detail__record_valid_from)::BLOB AS _pit_hook__sales_order_detail,
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