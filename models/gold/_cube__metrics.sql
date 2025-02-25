MODEL (
  kind VIEW
);

WITH one_big_table AS (
  SELECT
    *
  FROM gold._bridge__as_of_event
  LEFT JOIN gold.addresses USING (_pit_hook__address)
  LEFT JOIN gold.calendar USING (_hook__calendar__date)
  LEFT JOIN gold.credit_cards USING (_pit_hook__credit_card)
  LEFT JOIN gold.currency_rates USING (_pit_hook__currency_rate)
  LEFT JOIN gold.customers USING (_pit_hook__customer)
  LEFT JOIN gold.product_categories USING (_pit_hook__product_category)
  LEFT JOIN gold.product_subcategories USING (_pit_hook__product_subcategory)
  LEFT JOIN gold.products USING (_pit_hook__product)
  LEFT JOIN gold.sales_order_details USING (_pit_hook__sales_order_detail)
  LEFT JOIN gold.sales_order_headers USING (_pit_hook__sales_order)
  LEFT JOIN gold.sales_persons USING (_pit_hook__sales_person)
  LEFT JOIN gold.sales_territories USING (_pit_hook__territory)
  LEFT JOIN gold.ship_methods USING (_pit_hook__ship_method)
  LEFT JOIN gold.special_offers USING (_pit_hook__special_offer)
  LEFT JOIN gold.state_provinces USING (_pit_hook__state_province)
), metrics AS (
  SELECT
    RIGHT(
      GROUPING_ID(
        year,
        year_week,
        weekday__name,
        date,
        territory__name
      )::BIT::TEXT,
      5
    ) AS grouping_id,
    TRIM(
      CONCAT_WS(', ',
       CASE WHEN SUBSTRING(grouping_id, 1, 1) = '0' THEN 'year' END,
       CASE WHEN SUBSTRING(grouping_id, 2, 1) = '0' THEN 'year_week' END,
       CASE WHEN SUBSTRING(grouping_id, 3, 1) = '0' THEN 'weekday__name' END,
       CASE WHEN SUBSTRING(grouping_id, 4, 1) = '0' THEN 'date' END,
       CASE WHEN SUBSTRING(grouping_id, 5, 1) = '0' THEN 'territory__name' END
      )
    ) AS grouped_by,
    -- Dimensions
    year_week,
    weekday__name,
    date,
    territory__name,
    -- Metrics
    SUM(measure__is_returning_customer) AS metric__orders_from_returning_customers,
    SUM(measure__sales_order_detail__placed) AS metric__sales_order_details_placed,
    SUM(measure__sales_order_detail__has_special_offer) AS metric__sales_order_lines_with_special_offer,
    SUM(measure__sales_order_detail__discount_price) AS metric__total_sales_order_discount_price,
    SUM(measure__sales_order_detail__price) AS metric__total_sales_order_price,
    SUM(measure__sales_order_detail__discount) AS metric__total_sales_order_discount,
    SUM(measure__sales_order_placed) AS metric__sales_orders_placed,
    MEAN(measure__sales_order_due_lead_time) AS metric__mean_sales_order_due_lead_time,
    MEAN(measure__sales_order_shipping_lead_time) AS metric__mean_sales_order_shipping_lead_time,
    SUM(measure__sales_order_due) AS metric__sales_orders_due,
    SUM(measure__sales_order_shipped_on_time) AS metric__sales_order_shipped_on_time,
    SUM(measure__sales_order_shipped) AS metric__sales_orders_shipped,
    -- Derived Metrics
    metric__sales_order_lines_with_special_offer/metric__sales_order_details_placed*100 AS metric__percentage_of_order_details_with_special_offer,
    metric__total_sales_order_discount/metric__total_sales_order_price*100 AS metric__percentage_of_sales_order_discount,
    metric__orders_from_returning_customers/metric__sales_orders_placed*100 AS metric__percentage_of_orders_from_returning_customers
  FROM one_big_table
  GROUP BY GROUPING SETS (
    (year),
    (year, year_week),
    (year, year_week, weekday__name, date),
    (year, territory__name),
    (year, year_week, territory__name),
    (year, year_week, weekday__name, date, territory__name),
  )
)

SELECT
  * EXCLUDE(grouping_id)
FROM metrics