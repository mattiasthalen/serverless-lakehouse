MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    product_photo_id,
    large_photo AS product_photo__large_photo,
    large_photo_file_name AS product_photo__large_photo_file_name,
    modified_date AS product_photo__modified_date,
    thumb_nail_photo AS product_photo__thumb_nail_photo,
    thumbnail_photo_file_name AS product_photo__thumbnail_photo_file_name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_photo__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_photos")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_photo_id ORDER BY product_photo__record_loaded_at) AS product_photo__record_version,
    CASE
      WHEN product_photo__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_photo__record_loaded_at
    END AS product_photo__record_valid_from,
    COALESCE(
      LEAD(product_photo__record_loaded_at) OVER (PARTITION BY product_photo_id ORDER BY product_photo__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_photo__record_valid_to,
    product_photo__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_photo__is_current_record,
    CASE WHEN product_photo__is_current_record THEN product_photo__record_loaded_at ELSE product_photo__record_valid_to END AS product_photo__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_photo|adventure_works|', product_photo_id, '~epoch|valid_from|', product_photo__record_valid_from)::BLOB AS _pit_hook__product_photo,
    CONCAT('product_photo|adventure_works|', product_photo_id)::BLOB AS _hook__product_photo,
    *
  FROM validity
)
SELECT
  *
FROM hooks