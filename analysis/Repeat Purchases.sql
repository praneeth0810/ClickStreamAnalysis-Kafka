-- User Buying Same Item Multiple Times
SELECT user_id, item_name, COUNT(*) AS times_bought
FROM order_details_enhanced
GROUP BY user_id, item_name
HAVING COUNT(*) > 1
ORDER BY times_bought DESC;
