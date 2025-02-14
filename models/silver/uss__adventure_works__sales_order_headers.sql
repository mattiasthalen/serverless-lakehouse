MODEL (
  kind VIEW
);

SELECT
  'adventure_works__sales_order_headers' AS stage,
  _pit_hook__sales_order,
  _hook__bill_to_address,
  _hook__credit_card,
  _hook__currency_rate,
  _hook__customer,
  _hook__sales_order,
  _hook__sales_person,
  _hook__ship_method,
  _hook__ship_to_address,
  _hook__territory,
  _sqlmesh__loaded_at,
  _sqlmesh__version,
  _sqlmesh__valid_from,
  _sqlmesh__valid_to,
  _sqlmesh__is_current,
  _sqlmesh__updated_at
FROM silver.bag__adventure_works__sales_order_headers