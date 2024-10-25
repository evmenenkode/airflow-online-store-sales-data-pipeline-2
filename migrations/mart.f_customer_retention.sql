INSERT INTO mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
WITH customer_orders AS (
    SELECT
        customer_id,
        item_id,
        COUNT(uniq_id) AS order_count,
        SUM(payment_amount) AS total_revenue,
        SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_count
    FROM staging.user_order_log
    WHERE date_time >= date_trunc('week', CURRENT_DATE) - interval '7 days' AND date_time < date_trunc('week', CURRENT_DATE)
    GROUP BY customer_id, item_id
),
customer_metrics AS (
    SELECT
        item_id,
        COUNT(CASE WHEN order_count = 1 THEN customer_id END) AS new_customers_count,
        COUNT(CASE WHEN order_count > 1 THEN customer_id END) AS returning_customers_count,
        COUNT(CASE WHEN refunded_count > 0 THEN customer_id END) AS refunded_customer_count,
        SUM(CASE WHEN order_count = 1 THEN total_revenue ELSE 0 END) AS new_customers_revenue,
        SUM(CASE WHEN order_count > 1 THEN total_revenue ELSE 0 END) AS returning_customers_revenue,
        SUM(refunded_count) AS customers_refunded
    FROM customer_orders
    GROUP BY item_id
)
SELECT 
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    'weekly' AS period_name,
    EXTRACT(WEEK FROM CURRENT_DATE) AS period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
FROM customer_metrics;