MODEL (
  kind VIEW
);

WITH cte__source AS (
    SELECT
        *
    FROM delta_scan("./lakehouse/bronze/raw__adventure_works__sales_order_details")
)
SELECT * FROM cte__source
