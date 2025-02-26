MODEL (
  kind VIEW
);

WITH cte__aggregated_transactions AS (
  SELECT
    _hook__product,
    product_id,
    transaction__transaction_date,
    transaction__quantity,
    SUM(
      CASE WHEN transaction__transaction_type = 'P' THEN transaction__quantity ELSE 0 END
    ) AS inventory__quantity_purchased,
    SUM(
      CASE WHEN transaction__transaction_type = 'W' THEN transaction__quantity ELSE 0 END
    ) AS inventory__quantity_made,
    SUM(
      CASE WHEN transaction__transaction_type = 'S' THEN transaction__quantity ELSE 0 END
    ) AS inventory__quantity_sold,
    inventory__quantity_purchased + inventory__quantity_made - inventory__quantity_sold AS inventory__net_transacted_quantity,
    MIN(transaction__record_loaded_at) AS inventory__record_loaded_at,
    MAX(transaction__record_updated_at) AS inventory__record_updated_at,
    MAX(transaction__record_valid_from) AS inventory__record_valid_from,
    MAX(transaction__record_valid_to) AS inventory__record_valid_to
  FROM silver.bag__adventure_works__transactions
  WHERE
    transaction__is_current_record = TRUE
  GROUP BY ALL
), cte__window AS (
  SELECT
    *,
    LEAD(transaction__transaction_date) OVER (PARTITION BY _hook__product ORDER BY transaction__transaction_date) AS transaction__next_transaction_date,
    SUM(inventory__net_transacted_quantity) OVER (PARTITION BY _hook__product ORDER BY transaction__transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS inventory__net_on_hand_quantity
  FROM cte__aggregated_transactions
), cte__expanded AS (
  SELECT
    *,
    UNNEST(
      GENERATE_SERIES(
        transaction__transaction_date,
        transaction__next_transaction_date - INTERVAL '1' DAY,
        INTERVAL '1' DAY
      )
    )::DATE AS inventory__inventory_date
  FROM cte__window
), cte__final AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product_id,
      '~epoch|inventory_date|',
      inventory__inventory_date,
      '~epoch|valid_from|',
      inventory__record_valid_from
    )::BLOB AS _pit_hook__inventory,
    CONCAT(
      'product|adventure_works|',
      product_id,
      '~epoch|inventory_date|',
      inventory__inventory_date
    )::BLOB AS _hook__inventory,
    _hook__product,
    inventory__inventory_date,
    CASE
      WHEN inventory__inventory_date = transaction__transaction_date
      THEN inventory__quantity_purchased
      ELSE 0
    END AS inventory__quantity_purchased,
    CASE
      WHEN inventory__inventory_date = transaction__transaction_date
      THEN inventory__quantity_made
      ELSE 0
    END AS inventory__quantity_made,
    CASE
      WHEN inventory__inventory_date = transaction__transaction_date
      THEN inventory__quantity_sold
      ELSE 0
    END AS inventory__quantity_sold,
    CASE
      WHEN inventory__inventory_date = transaction__transaction_date
      THEN inventory__net_transacted_quantity
      ELSE 0
    END AS inventory__net_transacted_quantity,
    COALESCE(
      LAG(inventory__net_on_hand_quantity) OVER (PARTITION BY _hook__product ORDER BY inventory__inventory_date),
      0
    ) AS inventory__gross_on_hand_quantity,
    inventory__net_on_hand_quantity,
    inventory__record_loaded_at,
    inventory__record_updated_at,
    inventory__record_valid_from,
    inventory__record_valid_to
  FROM cte__expanded
  ORDER BY
    _hook__inventory
)
SELECT
  *
FROM cte__final