MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    product_category_id,
    modified_date,
    name,
    rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__product_categories")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_category_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY product_category_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product_category|adventure_works|', product_category_id, '~epoch|valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__product_category,
    CONCAT('product_category|adventure_works|', product_category_id)::BLOB AS _hook__product_category,
    *
  FROM validity
)
SELECT
  *
FROM hooks