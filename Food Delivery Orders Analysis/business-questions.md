
---

# Top 1 outlets by cuisine type without using limit and top 

---

### Query

```sql

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

```

### Output

| restaurant\_id | cuisine  | orders |
| -------------- | -------- | ------ |
| BURGER99       | American | 8      |
| PIZZA123       | Italian  | 10     |
| SUSHI456       | Japanese | 6      |
| KMKMH6787      | Lebanese | 10     |
| TACO789        | Mexican  | 7      |


---

## Find the daily new customer count from the launch date(everyday how many new customers are we acquiring)

---

### Query

```sql

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

```

### Output

| day        | new\_users |
| ---------- | ---------- |
| 2025-01-01 | 2          |
| 2025-01-02 | 1          |
| 2025-01-03 | 1          |
| 2025-01-04 | 1          |
| 2025-01-05 | 3          |
| 2025-01-06 | 1          |
| 2025-01-07 | 1          |
| 2025-01-08 | 1          |
| 2025-01-09 | 1          |
| 2025-01-10 | 3          |
| 2025-01-11 | 1          |
| 2025-01-12 | 1          |
| 2025-01-13 | 1          |
| 2025-01-14 | 1          |
| 2025-01-15 | 2          |
| 2025-01-16 | 1          |
| 2025-01-17 | 1          |
| 2025-01-18 | 1          |
| 2025-01-19 | 1          |
| 2025-01-20 | 2          |
| 2025-01-21 | 1          |
| 2025-01-22 | 1          |
| 2025-01-23 | 1          |
| 2025-01-24 | 1          |
| 2025-01-25 | 1          |
| 2025-01-26 | 1          |
| 2025-01-27 | 1          |
| 2025-01-28 | 1          |
| 2025-01-29 | 1          |
| 2025-01-30 | 1          |
| 2025-01-31 | 4          |
| 2025-02-01 | 2          |
| 2025-02-05 | 1          |
| 2025-02-10 | 1          |
| 2025-03-20 | 2          |

---

## Count of all the users who were acquired in Jan 2025 and only placed one order in Jan and did not place any other order.

---

### Query

```sql

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

```

### Output

| customers |
| --------- |
| 35        |

---

## List all the customers with no order in the last 7 days but were acquired one month ago with their first on promo.

---

### Query

```sql

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

```

### Output

|      user_id      |
| ----------------- |
|  ABC1234567890XYZ |
|  DEF9876543210XYZ |
|  GHI5678901234XYZ |
|  JKL3456789012XYZ |
|  PQR1234567890ABC |
|  VWX5678901234ABC |
|  BCD7890123456ABC |
|  HIJ9876543210DEF |
|  QRS7890123456DEF |
|  WXY9876543210GHI |
|  FGH7890123456GHI |
|  LMN9876543210JKL |
|  ABC9876543210MNO |
|  JKL7890123456MNO |
|  PQR9876543210PQR |
|  JAN_ONLY_ORDER1  |
|  JAN_ONLY_ORDER2  |
|  NO_ORDER_LAST7_1 |
|  NO_ORDER_LAST7_2 |
| THIRD_ORDER_CUST1 |
| THIRD_ORDER_CUST2 | 
|  SINGLE_ORDER_JAN |
|  NO_ORDER_RECENT  |
|  PROMO_FIRST_ONLY |


---

## Growth team is planning to create a trigger that will target customers after their every third order with a personalized communication and they asked to create a query for this.

---

### Query

```sql

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

```

---

## List the customers who placed more than 1 order and all their orders on a promo only.

---

### Query

```sql

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

```

### Output

| user\_id         |
| ---------------- |
| DEF9876543210XYZ |
| UVW7890123456JKL |


---

## What percent of customers were originally aquired in jan2025. (placed their first order without promo code).

---

### Query

```sql

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

```

### Output

|   customer_ratio  |
| ----------------- |
| 43.90243902439025 |