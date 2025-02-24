SELECT
  _pit_hook__sales_order,
  UNNEST(GENERATE_SERIES(sales_order__ship_date, sales_order__due_date, INTERVAL 1 DAY))::DATE AS event_date
FROM silver.bag__adventure_works__sales_order_headers
WHERE _pit_hook__sales_order = 'sales_order|adventure_works|75107~epoch|valid_from|1970-01-01 00:00:00+01'
;