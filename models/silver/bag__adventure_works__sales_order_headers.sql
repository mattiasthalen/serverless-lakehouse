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
    account_number AS sales_order__account_number,
    credit_card_approval_code AS sales_order__credit_card_approval_code,
    due_date::DATE AS sales_order__due_date,
    freight AS sales_order__freight,
    modified_date::DATE AS sales_order__modified_date,
    online_order_flag AS sales_order__online_order_flag,
    order_date::DATE AS sales_order__order_date,
    purchase_order_number AS sales_order__purchase_order_number,
    revision_number AS sales_order__revision_number,
    rowguid AS sales_order__rowguid,
    sales_order_number AS sales_order__sales_order_number,
    ship_date::DATE AS sales_order__ship_date,
    status AS sales_order__status,
    sub_total AS sales_order__sub_total,
    tax_amt AS sales_order__tax_amt,
    total_due AS sales_order__total_due,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sales_order__order_date) AS sales_order__customer_order_sequence,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order__record_loaded_at
  FROM bronze.raw__adventure_works__sales_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_id ORDER BY sales_order__record_loaded_at) AS sales_order__record_version,
    CASE
      WHEN sales_order__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_order__record_loaded_at
    END AS sales_order__record_valid_from,
    COALESCE(
      LEAD(sales_order__record_loaded_at) OVER (PARTITION BY sales_order_id ORDER BY sales_order__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_order__record_valid_to,
    sales_order__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_order__is_current_record,
    CASE
      WHEN sales_order__is_current_record
      THEN sales_order__record_loaded_at
      ELSE sales_order__record_valid_to
    END AS sales_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order|adventure_works|',
      sales_order_id,
      '~epoch|valid_from|',
      sales_order__record_valid_from
    )::BLOB AS _pit_hook__sales_order,
    CONCAT('sales_order|adventure_works|', sales_order_id)::BLOB AS _hook__sales_order,
    CONCAT('address|adventure_works|', bill_to_address_id)::BLOB AS _hook__address__bill_to,
    CONCAT('credit_card|adventure_works|', credit_card_id)::BLOB AS _hook__credit_card,
    CONCAT('currency_rate|adventure_works|', currency_rate_id)::BLOB AS _hook__currency_rate,
    CONCAT('customer|adventure_works|', customer_id)::BLOB AS _hook__customer,
    CONCAT('sales_person|adventure_works|', sales_person_id)::BLOB AS _hook__sales_person,
    CONCAT('ship_method|adventure_works|', ship_method_id)::BLOB AS _hook__ship_method,
    CONCAT('address|adventure_works|', ship_to_address_id)::BLOB AS _hook__address__ship_to,
    CONCAT('territory|adventure_works|', territory_id)::BLOB AS _hook__territory,
    *
  FROM validity
)
SELECT
  *
FROM hooks