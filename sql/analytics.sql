-- Databricks SQL analytics queries using target Delta tables.
-- Replace workspace.bi_assignment with your catalog.schema if different.

-- a. Daily active users by region
SELECT f.order_date, c.region, COUNT(DISTINCT f.customer_id) AS daily_active_users
FROM workspace.bi_assignment.fact_order_line f
JOIN workspace.bi_assignment.dim_customer c ON f.customer_id = c.customer_id
GROUP BY f.order_date, c.region
ORDER BY f.order_date, c.region;

-- b. Revenue with conversion/order count/quantity for the Sweet category
SELECT
    p.category_group,
    ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS total_quantity,
    ROUND(COUNT(DISTINCT f.order_id) / COUNT(DISTINCT f.customer_id), 4) AS orders_per_customer_conversion_proxy
FROM workspace.bi_assignment.fact_order_line f
JOIN workspace.bi_assignment.dim_product p ON f.product_id = p.product_id
WHERE LOWER(p.category_group) = 'sweet'
GROUP BY p.category_group;

-- c. Top 3 products by revenue and by region
WITH product_region AS (
    SELECT
        c.region,
        p.product_id,
        p.product_name,
        ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
        DENSE_RANK() OVER (PARTITION BY c.region ORDER BY SUM(f.revenue_usd) DESC) AS revenue_rank
    FROM workspace.bi_assignment.fact_order_line f
    JOIN workspace.bi_assignment.dim_customer c ON f.customer_id = c.customer_id
    JOIN workspace.bi_assignment.dim_product p ON f.product_id = p.product_id
    GROUP BY c.region, p.product_id, p.product_name
)
SELECT region, revenue_rank, product_id, product_name, revenue_usd
FROM product_region
WHERE revenue_rank <= 3
ORDER BY region, revenue_rank, product_name;

-- d. Customer lifetime value proxy
SELECT
    c.customer_id,
    c.region,
    c.customer_type,
    c.is_active,
    ROUND(SUM(f.revenue_usd), 2) AS lifetime_revenue_usd,
    COUNT(DISTINCT f.order_id) AS lifetime_order_count,
    SUM(f.quantity) AS lifetime_quantity
FROM workspace.bi_assignment.fact_order_line f
JOIN workspace.bi_assignment.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.region, c.customer_type, c.is_active
ORDER BY lifetime_revenue_usd DESC;

-- e. Duplicate orders and faulty transactions
SELECT
    'duplicate_order_line' AS issue_type,
    order_id,
    customer_id,
    CAST(order_ts AS STRING) AS order_ts,
    product_id,
    CAST(quantity AS STRING) AS quantity,
    order_status,
    CONCAT('duplicate_count=', duplicate_count) AS issue_detail
FROM workspace.bi_assignment.audit_duplicate_order_lines
UNION ALL
SELECT
    'faulty_transaction' AS issue_type,
    order_id,
    customer_id,
    CAST(order_ts AS STRING) AS order_ts,
    product_id,
    CAST(quantity AS STRING) AS quantity,
    order_status,
    fault_reason AS issue_detail
FROM workspace.bi_assignment.audit_faulty_transactions
ORDER BY issue_type, order_id, product_id;
