INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
SELECT
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    CASE
        WHEN uol.status = 'refunded' THEN -uol.quantity
        ELSE uol.quantity
    END AS quantity,
    CASE
        WHEN uol.status = 'refunded' THEN -uol.payment_amount
        ELSE uol.payment_amount
    END AS payment_amount
FROM
    staging.user_order_log uol
LEFT JOIN
    mart.d_calendar AS dc ON uol.date_time::date = dc.date_actual
WHERE
    uol.date_time::date = '{{ ds }}'
    AND (uol.status = 'shipped' OR uol.status = 'refunded');