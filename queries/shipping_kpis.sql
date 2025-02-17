SELECT
    calendar.date,
    SUM(_bridge__as_of_event.measure__sales_order_placed) AS metric__sales_orders_placed,
    AVG(_bridge__as_of_event.measure__sales_order_due_lead_time) AS metric__avg_sales_order_due_lead_time,
    AVG(_bridge__as_of_event.measure__sales_order_shipping_lead_time) AS metric__avg_sales_order_shipping_lead_time,
    SUM(_bridge__as_of_event.measure__sales_order_due) AS metric__sales_orders_due,
    SUM(_bridge__as_of_event.measure__sales_order_shipped_on_time) AS metric__sales_orders_shipped_on_time,
    SUM(_bridge__as_of_event.measure__sales_order_shipped) AS metric__sales_orders_shipped,

    QUANTILE_CONT(metric__sales_orders_shipped, 0.90) OVER () AS metric__shipping_capacity,
    QUANTILE_CONT(metric__avg_sales_order_shipping_lead_time, 0.90) OVER () AS metric__shipping_lead_time_capacity,
    
    metric__sales_orders_due / metric__shipping_capacity AS kpi__shipping_load_ratio,
    metric__avg_sales_order_due_lead_time / metric__shipping_lead_time_capacity AS kpi__shipping_lead_time_load_ratio,
    metric__sales_orders_shipped_on_time / metric__sales_orders_due AS kpi__on_time_shipping
    
FROM gold._bridge__as_of_event
LEFT JOIN gold.calendar USING (_hook__calendar__date)
GROUP BY calendar.date
;