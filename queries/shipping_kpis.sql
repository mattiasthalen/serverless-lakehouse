SELECT
    calendar.date,
    SUM(_bridge__as_of_event.measure__sales_order_placed) AS metric__sales_orders_placed,
    SUM(_bridge__as_of_event.measure__sales_order_due) AS metric__sales_orders_due,
    SUM(_bridge__as_of_event.measure__sales_order_shipped_on_time) AS metric__sales_orders_shipped_on_time,
    SUM(_bridge__as_of_event.measure__sales_order_shipped) AS metric__sales_orders_shipped,

    QUANTILE_CONT(total_sales_orders_shipped, 0.90) OVER () AS metric__shipping_baseline_capacity,
    
    total_sales_orders_due / shipping_baseline_capacity AS kpi__shipping_load_ratio,
    total_sales_orders_shipped_on_time / total_sales_orders_due AS kpi__on_time_shipping
    
FROM gold._bridge__as_of_event
LEFT JOIN gold.calendar USING (_hook__calendar__date)
GROUP BY calendar.date
;