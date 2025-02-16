MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    operation_sequence AS operation_sequence__operation_sequence,
    location_id,
    product_id,
    work_order_id,
    actual_cost AS operation_sequence__actual_cost,
    actual_end_date AS operation_sequence__actual_end_date,
    actual_resource_hrs AS operation_sequence__actual_resource_hrs,
    actual_start_date AS operation_sequence__actual_start_date,
    modified_date AS operation_sequence__modified_date,
    planned_cost AS operation_sequence__planned_cost,
    scheduled_end_date AS operation_sequence__scheduled_end_date,
    scheduled_start_date AS operation_sequence__scheduled_start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS operation_sequence__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__work_order_routings")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY operation_sequence ORDER BY operation_sequence__record_loaded_at) AS operation_sequence__record_version,
    CASE
      WHEN operation_sequence__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE operation_sequence__record_loaded_at
    END AS operation_sequence__record_valid_from,
    COALESCE(
      LEAD(operation_sequence__record_loaded_at) OVER (PARTITION BY operation_sequence ORDER BY operation_sequence__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS operation_sequence__record_valid_to,
    operation_sequence__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS operation_sequence__is_current_record,
    CASE WHEN operation_sequence__is_current_record THEN operation_sequence__record_loaded_at ELSE operation_sequence__record_valid_to END AS operation_sequence__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('operation_sequence|adventure_works|', operation_sequence)::BLOB AS _hook__operation_sequence,
    CONCAT('location|adventure_works|', location_id)::BLOB AS _hook__location,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    CONCAT('work_order|adventure_works|', work_order_id)::BLOB AS _hook__work_order,
    *
  FROM validity
)
SELECT
  *
FROM hooks