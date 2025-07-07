SELECT location, item_name, SUM(item_price) AS revenue
FROM order_details_enhanced
WHERE dt = DATE '2025-07-05'
GROUP BY location, item_name
ORDER BY revenue DESC
LIMIT 20;
