MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    uss__int__sales_order_headers._pit_hook__sales_order,
    uss__int__sales_order_headers._pit_hook__customer,
    uss__int__sales_order_headers._pit_hook__store,
    uss__int__sales_order_headers._pit_hook__sales_person,
    uss__int__sales_order_headers._pit_hook__territory,
    uss__int__products._pit_hook__product,
    bag__adventure_works__sales_order_details._hook__sales_order_detail,
    bag__adventure_works__sales_order_details.sales_order_detail__carrier_tracking_number,
    bag__adventure_works__sales_order_details.sales_order_detail__line_total,
    bag__adventure_works__sales_order_details.sales_order_detail__modified_date,
    bag__adventure_works__sales_order_details.sales_order_detail__order_qty,
    bag__adventure_works__sales_order_details.sales_order_detail__rowguid,
    bag__adventure_works__sales_order_details.sales_order_detail__unit_price,
    bag__adventure_works__sales_order_details.sales_order_detail__unit_price_discount,
    bag__adventure_works__special_offers.special_offer__category,
    bag__adventure_works__special_offers.special_offer__description,
    bag__adventure_works__special_offers.special_offer__discount_percentage,
    bag__adventure_works__special_offers.special_offer__end_date,
    bag__adventure_works__special_offers.special_offer__maximum_quantity,
    bag__adventure_works__special_offers.special_offer__minimum_quantity,
    bag__adventure_works__special_offers.special_offer__modified_date,
    bag__adventure_works__special_offers.special_offer__rowguid,
    bag__adventure_works__special_offers.special_offer__start_date,
    bag__adventure_works__special_offers.special_offer__type,
    GREATEST(
      bag__adventure_works__sales_order_details.sales_order_detail__record_loaded_at,
      uss__int__sales_order_headers.sales_order__record_loaded_at,
      uss__int__products.product__record_loaded_at
    ) AS sales_order_detail__record_loaded_at,
    GREATEST(
      bag__adventure_works__sales_order_details.sales_order_detail__record_updated_at,
      uss__int__sales_order_headers.sales_order__record_updated_at,
      uss__int__products.product__record_updated_at
    ) AS sales_order_detail__record_updated_at,
    GREATEST(
      bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from,
      uss__int__sales_order_headers.sales_order__record_valid_from,
      uss__int__products.product__record_valid_from
    ) AS sales_order_detail__record_valid_from,
    LEAST(
      bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to,
      uss__int__sales_order_headers.sales_order__record_valid_to,
      uss__int__products.product__record_valid_to
    ) AS sales_order_detail__record_valid_to,
    sales_order_detail__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS store__is_current_record
  FROM silver.bag__adventure_works__sales_order_details
  LEFT JOIN silver.uss__int__products
    ON bag__adventure_works__sales_order_details._hook__product = uss__int__products._hook__product
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from < uss__int__products.product__record_valid_to
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to > uss__int__products.product__record_valid_from
  LEFT JOIN silver.uss__int__sales_order_headers
    ON bag__adventure_works__sales_order_details._hook__sales_order = uss__int__sales_order_headers._hook__sales_order
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from < uss__int__sales_order_headers.sales_order__record_valid_to
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to > uss__int__sales_order_headers.sales_order__record_valid_from
  LEFT JOIN silver.bag__adventure_works__special_offers
    ON bag__adventure_works__sales_order_details._hook__special_offer = bag__adventure_works__special_offers._hook__special_offer
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_from < bag__adventure_works__special_offers.special_offer__record_valid_to
    AND bag__adventure_works__sales_order_details.sales_order_detail__record_valid_to > bag__adventure_works__special_offers.special_offer__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(
      _hook__sales_order_detail::TEXT,
      '~epoch|valid_from',
      sales_order_detail__record_valid_from
    )::BLOB AS _pit_hook__sales_order_detail,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time