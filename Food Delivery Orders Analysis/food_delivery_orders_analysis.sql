
-- Q1. Top 1 outlets by cuisine type without using limit and top 

WITH cuisine_orders AS(
	SELECT 
		restaurant_id, cuisine, COUNT(order_id) orders
	FROM orders
	GROUP BY restaurant_id, cuisine
)

, ranked_cuisines AS (
	SELECT 
		*, ROW_NUMBER() OVER(PARTITION BY cuisine ORDER BY orders DESC) rn
	FROM cuisine_orders
)

SELECT 
    restaurant_id, cuisine, orders 
FROM ranked_cuisines 
WHERE rn = 1



-- Q2. Find the daily new customer count from the launch date(everyday how many new customers are we acquiring)

with user_records AS (
	SELECT 
		user_id, DATE(timestamp) AS day, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp)  rn
	FROM orders
)

SELECT 
	day, COUNT(DISTINCT user_id) new_users
FROM user_records 
WHERE rn = 1
GROUP BY day
ORDER BY day



-- Q3. Count of all the users who were acquired in Jan 2025 and only placed one order in Jan and did not place any other order.

with user_records AS (
	SELECT 
		user_id, DATE(timestamp) AS day, order_id, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp)  rn
	FROM orders
)

, jan_orders AS (
	SELECT 
		user_id, COUNT(order_id) orders
	FROM user_records 
	WHERE rn = 1 AND DATE_TRUNC('month', day) = '2025-01-01' AND user_id NOT IN (SELECT DISTINCT user_id FROM orders WHERE DATE(timestamp) >= DATE('2025-02-01'))
	GROUP BY user_id
	HAVING COUNT(order_id) = 1
)

SELECT 
	COUNT(DISTINCT user_id) AS customers 
FROM jan_orders



-- Q4. List all the customers with no order in the last 7 days but were acquired one month ago with their first on promo.

WITH cte AS (
    SELECT 
        user_id, 
        MIN(timestamp) AS first_order, 
        MAX(timestamp) AS latest_order
    FROM orders
    GROUP BY user_id
)
SELECT 
    c.user_id
FROM cte c
JOIN orders o 
    ON c.user_id = o.user_id 
   AND c.first_order = o.timestamp
WHERE 
    c.latest_order < DATE '2025-03-31' - INTERVAL '7 days'
    AND c.first_order < DATE '2025-03-31' - INTERVAL '1 month'
    AND o.promo_code IS NOT NULL;



-- Q5. Growth team is planning to create a trigger that will target customers after their every third order with a personalized communication and they asked to create a query for this.

WITH cte AS (
    SELECT 
        user_id, 
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) AS order_no
    FROM orders
)
SELECT 
    user_id, 
    timestamp
FROM cte
WHERE 
    order_no % 3 = 0
    AND CAST(timestamp AS DATE) = CURRENT_DATE;


-- Q6. List the customers who placed more than 1 order and all their orders on a promo only.

WITH customers_orders_summary AS (
	SELECT 
		user_id, 
        COUNT(order_id) total_orders, 
		COUNT(CASE WHEN promo_code IS NOT NULL THEN order_id END) promo_orders,
		COUNT(CASE WHEN promo_code IS NULL THEN order_id END) non_promo_orders
	FROM orders
	GROUP BY user_id
	HAVING COUNT(order_id) > 1
)

SELECT 
	DISTINCT user_id
FROM customers_orders_summary
WHERE promo_orders = total_orders



-- Q7. What percent of customers who were originally aquired in jan2025 and placed their first order without promo code.

with user_records AS (
	SELECT 
		user_id, 
		MIN(DATE(timestamp)) firstday
	FROM orders
	GROUP BY 1
	HAVING DATE_TRUNC('month', MIN(DATE(timestamp))) = DATE('2025-01-01') AND COUNT(promo_code) = 0
)

SELECT 
	(CAST(COUNT(DISTINCT user_id) AS FLOAT) / (SELECT COUNT(DISTINCT user_id) FROM orders WHERE DATE_TRUNC('month', DATE(timestamp)) = DATE('2025-01-01')) * 100.0) customer_ratio
FROM user_records 


