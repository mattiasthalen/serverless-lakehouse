MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    purchase_order_id,
    employee_id,
    ship_method_id,
    vendor_id,
    freight AS purchase_order__freight,
    modified_date AS purchase_order__modified_date,
    order_date AS purchase_order__order_date,
    revision_number AS purchase_order__revision_number,
    ship_date AS purchase_order__ship_date,
    status AS purchase_order__status,
    sub_total AS purchase_order__sub_total,
    tax_amt AS purchase_order__tax_amt,
    total_due AS purchase_order__total_due,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__purchase_order_headers")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order_id ORDER BY purchase_order__record_loaded_at) AS purchase_order__record_version,
    CASE
      WHEN purchase_order__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE purchase_order__record_loaded_at
    END AS purchase_order__record_valid_from,
    COALESCE(
      LEAD(purchase_order__record_loaded_at) OVER (PARTITION BY purchase_order_id ORDER BY purchase_order__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS purchase_order__record_valid_to,
    purchase_order__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS purchase_order__is_current_record,
    CASE WHEN purchase_order__is_current_record THEN purchase_order__record_loaded_at ELSE purchase_order__record_valid_to END AS purchase_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('purchase_order|adventure_works|', purchase_order_id, '~epoch|valid_from|', purchase_order__record_valid_from)::BLOB AS _pit_hook__purchase_order,
    CONCAT('purchase_order|adventure_works|', purchase_order_id)::BLOB AS _hook__purchase_order,
    CONCAT('employee|adventure_works|', employee_id)::BLOB AS _hook__employee,
    CONCAT('ship_method|adventure_works|', ship_method_id)::BLOB AS _hook__ship_method,
    CONCAT('vendor|adventure_works|', vendor_id)::BLOB AS _hook__vendor,
    *
  FROM validity
)
SELECT
  *
FROM hooks