MODEL (
  kind VIEW
);

WITH cte__source AS (
    SELECT
        *
    FROM delta_scan("./lakehouse/bronze/raw__adventure_works__products")
)
SELECT * FROM cte__source
