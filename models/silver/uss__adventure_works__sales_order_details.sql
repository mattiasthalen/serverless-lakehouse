MODEL (
  kind VIEW
);

SELECT
    _pit_hook__sales_order_detail,
    _hook__product,
    _hook__sales_order,
    _hook__sales_order_detail,
    _hook__special_offer,    
    _sqlmesh__loaded_at,
    _sqlmesh__version,
    _sqlmesh__valid_from,
    _sqlmesh__valid_to,
    _sqlmesh__is_current,
    _sqlmesh__updated_at
FROM silver.bag__adventure_works__sales_order_details
