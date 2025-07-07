SELECT user_id, SUM(order_amount) AS total_spent
FROM order_details_enhanced
WHERE dt BETWEEN DATE '2025-07-01' AND DATE '2025-07-07'
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;
