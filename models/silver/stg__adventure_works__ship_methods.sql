MODEL (
  kind VIEW
);

SELECT
   ship_method_id,
   name,
   ship_base,
   ship_rate,
   rowguid,
   modified_date,
   TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at,
   ROW_NUMBER() OVER (PARTITION BY ship_method_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
   CASE
       WHEN _sqlmesh__version = 1
       THEN '1970-01-01 00:00:00'::TIMESTAMP
       ELSE _sqlmesh__loaded_at
   END AS _sqlmesh__valid_from,
   COALESCE(
       LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY ship_method_id ORDER BY _sqlmesh__loaded_at),
       '9999-12-31 23:59:59'::TIMESTAMP
   ) AS _sqlmesh__valid_to,
   _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
   CASE
       WHEN _sqlmesh__is_current
       THEN _sqlmesh__loaded_at
       ELSE _sqlmesh__valid_to
   END AS _sqlmesh__updated_at
FROM delta_scan("./lakehouse/bronze/raw__adventure_works__ship_methods")