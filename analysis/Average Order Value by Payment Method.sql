SELECT payment_method, ROUND(AVG(order_amount), 2) AS avg_order_value
FROM order_details_enhanced
WHERE dt = DATE '2025-07-05'
GROUP BY payment_method;
