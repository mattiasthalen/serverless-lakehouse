MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    sales_order_id,
    bill_to_address_id,
    credit_card_id,
    currency_rate_id,
    customer_id,
    sales_person_id,
    ship_method_id,
    ship_to_address_id,
    territory_id,
    account_number,
    credit_card_approval_code,
    due_date,
    freight,
    modified_date,
    online_order_flag,
    order_date,
    purchase_order_number,
    revision_number,
    rowguid,
    sales_order_number,
    ship_date,
    status,
    sub_total,
    tax_amt,
    total_due,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/raw__adventure_works__sales_order_headers")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY sales_order_id ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order|adventure_works|',
      sales_order_id,
      '~epoch|valid_from|',
      _sqlmesh__valid_from
    )::BLOB AS _pit_hook__sales_order,
    CONCAT('sales_order|adventure_works|', sales_order_id)::BLOB AS _hook__sales_order,
    CONCAT('bill_to_address|adventure_works|', bill_to_address_id)::BLOB AS _hook__bill_to_address,
    CONCAT('credit_card|adventure_works|', credit_card_id)::BLOB AS _hook__credit_card,
    CONCAT('currency_rate|adventure_works|', currency_rate_id)::BLOB AS _hook__currency_rate,
    CONCAT('customer|adventure_works|', customer_id)::BLOB AS _hook__customer,
    CONCAT('sales_person|adventure_works|', sales_person_id)::BLOB AS _hook__sales_person,
    CONCAT('ship_method|adventure_works|', ship_method_id)::BLOB AS _hook__ship_method,
    CONCAT('ship_to_address|adventure_works|', ship_to_address_id)::BLOB AS _hook__ship_to_address,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks