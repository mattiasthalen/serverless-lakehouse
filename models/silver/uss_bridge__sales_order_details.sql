MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'sales_order_details' AS stage,
    bag__adventure_works__sales_order_details._pit_hook__sales_order_detail,
    bag__adventure_works__sales_order_details._hook__sales_order_detail,
    uss_bridge__products._pit_hook__product,
    uss_bridge__products._pit_hook__product_subcategory,
    uss_bridge__products._pit_hook__product_category,
    uss_bridge__sales_order_headers._pit_hook__sales_order,
    uss_bridge__sales_order_headers._pit_hook__credit_card,
    uss_bridge__sales_order_headers._pit_hook__currency_rate,
    uss_bridge__sales_order_headers._pit_hook__customer,
    uss_bridge__sales_order_headers._pit_hook__sales_person,
    uss_bridge__sales_order_headers._pit_hook__ship_method,
    uss_bridge__sales_order_headers._pit_hook__address,
    uss_bridge__sales_order_headers._pit_hook__state_province,
    uss_bridge__sales_order_headers._pit_hook__territory,
    uss_bridge__special_offers._pit_hook__special_offer,
    GREATEST(
        bag__adventure_works__sales_order_details.sales_order_detail__record_loaded_at,
        uss_bridge__sales_order_headers.bridge__record_loaded_at,
        uss_bridge__special_offers.bridge__record_loaded_at,
        uss_bridge__products.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        bag__adventure_works__sales_order_details.sales_order_detail__record_updated_at,
        uss_bridge__sales_order_headers.bridge__record_updated_at,
        uss_bridge__special_offers.bridge__record_updated_at,
        uss_bridge__products.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from,
        uss_bridge__sales_order_headers.bridge__record_valid_from,
        uss_bridge__special_offers.bridge__record_valid_from,
        uss_bridge__products.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to,
        uss_bridge__sales_order_headers.bridge__record_valid_to,
        uss_bridge__special_offers.bridge__record_valid_to,
        uss_bridge__products.bridge__record_valid_to
    ) AS bridge__record_valid_to
  FROM silver.bag__adventure_works__sales_order_details
  LEFT JOIN silver.uss_bridge__products
    ON bag__adventure_works__sales_order_details._hook__product = uss_bridge__products._hook__product
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from <= uss_bridge__products.bridge__record_valid_to
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to >= uss_bridge__products.bridge__record_valid_from
  LEFT JOIN silver.uss_bridge__sales_order_headers
      ON bag__adventure_works__sales_order_details._hook__sales_order = uss_bridge__sales_order_headers._hook__sales_order
      AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from <= uss_bridge__sales_order_headers.bridge__record_valid_to
      AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to >= uss_bridge__sales_order_headers.bridge__record_valid_from
  LEFT JOIN silver.uss_bridge__special_offers
      ON bag__adventure_works__sales_order_details._hook__special_offer = uss_bridge__special_offers._hook__special_offer
      AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from <= uss_bridge__special_offers.bridge__record_valid_to
      AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to >= uss_bridge__special_offers.bridge__record_valid_from
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge