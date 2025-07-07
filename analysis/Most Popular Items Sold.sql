SELECT item_name, COUNT(*) AS times_sold
FROM order_details_enhanced
WHERE dt BETWEEN DATE '2025-07-01' AND DATE '2025-07-07'
GROUP BY item_name
ORDER BY times_sold DESC
LIMIT 10;
