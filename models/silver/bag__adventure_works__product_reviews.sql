MODEL (
  kind VIEW
);
    
WITH staging AS (
  SELECT
    product_review_id,
    product_id,
    comments AS product_review__comments,
    email_address AS product_review__email_address,
    modified_date AS product_review__modified_date,
    rating AS product_review__rating,
    review_date AS product_review__review_date,
    reviewer_name AS product_review__reviewer_name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_review__record_loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_reviews")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_review_id ORDER BY product_review__record_loaded_at) AS product_review__record_version,
    CASE
      WHEN product_review__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_review__record_loaded_at
    END AS product_review__record_valid_from,
    COALESCE(
      LEAD(product_review__record_loaded_at) OVER (PARTITION BY product_review_id ORDER BY product_review__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_review__record_valid_to,
    product_review__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_review__is_current_record,
    CASE WHEN product_review__is_current_record THEN product_review__record_loaded_at ELSE product_review__record_valid_to END AS product_review__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_review|adventure_works|', product_review_id)::BLOB AS _hook__product_review,
    CONCAT('product|adventure_works|', product_id)::BLOB AS _hook__product,
    *
  FROM validity
)
SELECT
  *
FROM hooks