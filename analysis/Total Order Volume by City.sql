SELECT location, COUNT(DISTINCT order_id) AS total_orders
FROM order_details_enhanced
WHERE dt = DATE '2025-07-05'
GROUP BY location
ORDER BY total_orders DESC
LIMIT 10;
