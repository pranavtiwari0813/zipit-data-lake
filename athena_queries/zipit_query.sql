-- ============================================================
-- ZIPIT Food Delivery — 8 Business SQL Queries
-- Database: zipit_db | Run in: Amazon Athena
-- ============================================================

-- Q1: Revenue by City
SELECT city, COUNT(order_id) AS total_orders,
       SUM(total_amount) AS total_revenue_rs,
       ROUND(AVG(total_amount),2) AS avg_order_value,
       COUNT(DISTINCT customer_id) AS unique_customers
FROM zipit_db.processed_orders
WHERE status = 'delivered'
GROUP BY city ORDER BY total_revenue_rs DESC;

-- Q2: Top 10 Restaurants
SELECT r.name AS restaurant_name, r.city, r.cuisine,
       COUNT(o.order_id) AS total_orders,
       SUM(o.total_amount) AS total_gmv_rs,
       ROUND(AVG(o.customer_rating),2) AS avg_rating,
       ROUND(AVG(o.delivery_mins),1) AS avg_del_mins
FROM zipit_db.processed_orders o
JOIN zipit_db.processed_restaurants r
    ON o.restaurant_id = r.restaurant_id
WHERE o.status = 'delivered'
  AND o.customer_rating IS NOT NULL
GROUP BY r.name, r.city, r.cuisine
ORDER BY total_orders DESC LIMIT 10;

-- Q3: Delivery Speed by City
SELECT city,
       ROUND(AVG(delivery_mins),1) AS avg_del_mins,
       MIN(CAST(delivery_mins AS INT)) AS fastest_mins,
       MAX(CAST(delivery_mins AS INT)) AS slowest_mins,
       COUNT(*) AS delivered_orders
FROM zipit_db.processed_orders
WHERE status = 'delivered'
  AND delivery_mins IS NOT NULL
GROUP BY city ORDER BY avg_del_mins ASC;

-- Q4: Most Popular Food Items
SELECT item_name, COUNT(*) AS times_ordered,
       SUM(total_amount) AS total_revenue_rs,
       ROUND(AVG(item_price),2) AS avg_price_rs
FROM zipit_db.processed_orders
WHERE status = 'delivered'
GROUP BY item_name
ORDER BY times_ordered DESC LIMIT 10;

-- Q5: Payment Method Split
SELECT payment_method, COUNT(*) AS transactions,
       SUM(total_amount) AS total_value_rs,
       ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct_share
FROM zipit_db.processed_orders
WHERE status != 'cancelled'
GROUP BY payment_method ORDER BY transactions DESC;

-- Q6: Cancellation Rate by City
SELECT city, COUNT(*) AS total_orders,
       SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,
       ROUND(SUM(CASE WHEN status='cancelled'
             THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS cancel_rate_pct
FROM zipit_db.processed_orders
GROUP BY city ORDER BY cancel_rate_pct DESC;

-- Q7: Top 10 Riders
SELECT o.rider_id, r.name AS rider_name, r.city,
       r.vehicle_type, COUNT(o.order_id) AS deliveries,
       ROUND(AVG(o.delivery_mins),1) AS avg_del_mins,
       ROUND(AVG(o.customer_rating),2) AS avg_rating
FROM zipit_db.processed_orders o
JOIN zipit_db.processed_riders r ON o.rider_id = r.rider_id
WHERE o.status = 'delivered'
  AND o.rider_id IS NOT NULL AND o.rider_id != ''
  AND o.customer_rating IS NOT NULL
GROUP BY o.rider_id, r.name, r.city, r.vehicle_type
HAVING COUNT(o.order_id) >= 3
ORDER BY deliveries DESC LIMIT 10;

-- Q8: Daily Growth Trend
SELECT DATE(date_parse(placed_at,'%Y-%m-%dT%H:%i:%s')) AS order_date,
       COUNT(*) AS orders_placed,
       SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,
       SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN status='delivered'
           THEN total_amount ELSE 0 END) AS daily_revenue_rs
FROM zipit_db.processed_orders
WHERE placed_at IS NOT NULL AND placed_at != ''
GROUP BY DATE(date_parse(placed_at,'%Y-%m-%dT%H:%i:%s'))
ORDER BY order_date ASC;