MODEL (
  kind VIEW,
  enabled FALSE
);
    
WITH staging AS (
  SELECT
    purchase_order_detail_id,
    product_id,
    purchase_order_id,
    due_date AS purchase_order_detail__due_date,
    line_total AS purchase_order_detail__line_total,
    modified_date AS purchase_order_detail__modified_date,
    order_qty AS purchase_order_detail__order_qty,
    received_qty AS purchase_order_detail__received_qty,
    rejected_qty AS purchase_order_detail__rejected_qty,
    stocked_qty AS purchase_order_detail__stocked_qty,
    unit_price AS purchase_order_detail__unit_price,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order_detail__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__purchase_order_details")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at) AS purchase_order_detail__record_version,
    CASE
      WHEN purchase_order_detail__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE purchase_order_detail__record_loaded_at
    END AS purchase_order_detail__record_valid_from,
    COALESCE(
      LEAD(purchase_order_detail__record_loaded_at) OVER (PARTITION BY purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS purchase_order_detail__record_valid_to,
    purchase_order_detail__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS purchase_order_detail__is_current_record,
    CASE WHEN purchase_order_detail__is_current_record THEN purchase_order_detail__record_loaded_at ELSE purchase_order_detail__record_valid_to END AS purchase_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('purchase_order_detail|adventure_works|', purchase_order_detail_id, '~epoch|valid_from|', purchase_order_detail__record_valid_from)::BLOB AS _pit_hook__purchase_order_detail,
    CONCAT('purchase_order_detail|adventure_works|', purchase_order_detail_id)::BLOB AS _hook__purchase_order_detail,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('purchase_order|adventure_works|', purchase_order_id)::BLOB AS _hook__purchase_order,
    *
  FROM validity
)
SELECT
  *
FROM hooks