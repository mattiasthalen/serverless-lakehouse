MODEL (
  kind VIEW
);

WITH cte__bridge AS (
  SELECT
    'inventories' AS stage,
    int__adventure_works__inventories._pit_hook__inventory,
    int__adventure_works__inventories._hook__inventory,
    uss_bridge__products._pit_hook__product,
    GREATEST(
      int__adventure_works__inventories.inventory__record_loaded_at,
      uss_bridge__products.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
      int__adventure_works__inventories.inventory__record_updated_at,
      uss_bridge__products.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
      int__adventure_works__inventories.inventory__record_valid_from,
      uss_bridge__products.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
      int__adventure_works__inventories.inventory__record_valid_to,
      uss_bridge__products.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.int__adventure_works__inventories
  LEFT JOIN silver.uss_bridge__products
    ON int__adventure_works__inventories._hook__product = uss_bridge__products._hook__product
    AND int__adventure_works__inventories.inventory__record_valid_from <= uss_bridge__products.bridge__record_valid_to
    AND int__adventure_works__inventories.inventory__record_valid_to >= uss_bridge__products.bridge__record_valid_from
), cte__measures AS (
  SELECT
    _pit_hook__inventory,
    CONCAT('calendar|date|', inventory__inventory_date)::BLOB AS _hook__calendar__date,
    inventory__quantity_purchased AS measure__inventory__quantity_purchased,
    inventory__quantity_made AS measure__inventory__quantity_made,
    inventory__quantity_sold AS measure__inventory__quantity_sold,
    inventory__net_transacted_quantity AS measure__inventory__net_transacted_quantity,
    inventory__gross_on_hand_quantity AS measure__inventory__gross_on_hand_quantity
  FROM silver.int__adventure_works__inventories
), cte__final AS (
  SELECT
    stage,
    _pit_hook__inventory,
    _hook__inventory,
    _pit_hook__product,
    _hook__calendar__date,
    measure__inventory__quantity_purchased,
    measure__inventory__quantity_made,
    measure__inventory__quantity_sold,
    measure__inventory__net_transacted_quantity,
    measure__inventory__gross_on_hand_quantity,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN cte__measures
    USING (_pit_hook__inventory)
)
SELECT
  *
FROM cte__final