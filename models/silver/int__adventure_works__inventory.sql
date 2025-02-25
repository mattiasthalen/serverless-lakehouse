MODEL (
  kind VIEW
);

WITH cte__aggregated_transactions AS (
  SELECT
    _hook__product,
    product_id,
    transaction__transaction_date AS inventory__inventory_date,
    SUM(
      CASE WHEN transaction__transaction_type = 'W' THEN transaction__quantity ELSE 0 END
    ) AS inventory__quantity_made,
    SUM(
      CASE WHEN transaction__transaction_type = 'S' THEN transaction__quantity ELSE 0 END
    ) AS inventory__quantity_sold,
    inventory__quantity_made - inventory__quantity_sold AS inventory__net_transaction_quantity
  FROM silver.bag__adventure_works__transactions
  WHERE
    1 = 1
    AND transaction__transaction_type IN ('W', 'S')
    AND transaction__is_current_record = TRUE
  GROUP BY ALL
), cte__cum_sum AS (
  SELECT
    *,
    SUM(inventory__net_transaction_quantity) OVER (PARTITION BY _hook__product ORDER BY inventory__inventory_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS inventory__net_on_hand_quantity
  FROM cte__aggregated_transactions
), cte__gross AS (
  SELECT
    *,
    LAG(inventory__net_on_hand_quantity, 1, 0) OVER (PARTITION BY _hook__product ORDER BY inventory__inventory_date) AS inventory__gross_on_hand_quantity
  FROM cte__cum_sum
), cte__final AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product_id,
      '~epoch|inventory_date|',
      inventory__inventory_date
    )::BLOB AS _hook__inventory,
    *
  FROM cte__gross
)
SELECT
  *
FROM cte__final
ORDER BY
  _hook__inventory