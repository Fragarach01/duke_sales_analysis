/* {{ config(materialized='view') }}

WITH tod AS (
    SELECT 
        *,
        EXTRACT(hour FROM time) AS hour_of_day
    FROM {{ ref('daily_sales_numbers') }}
)

SELECT 
    hour_of_day,
    AVG(number_of_accounts) as avg_number_of_accounts,
    AVG(total_sales) as avg_sales,
    AVG(average_account_value) as avg_account_value,
    AVG(total_discount) as avg_total_discounts,
    AVG(total_wastage) as avg_total_wastage
FROM tod
GROUP BY hour_of_day */




{{ config(materialized='view') }}


WITH waste AS (
    SELECT date, time, account_id, type, description, detail, sales_amount
    FROM {{ ref('transactions_clean') }}
    WHERE type = 'Wastage'
),

food as (
    SELECT date, account_id, description, payment_amount
    FROM {{ ref('transactions_clean') }}
    WHERE description ILIKE '%ICO%'
)

SELECT 
    EXTRACT(hour FROM time) AS hour_of_day,
    COUNT(DISTINCT(t.account_id)) AS number_of_accounts,
    SUM(t.payment_amount) AS total_sales,
    SUM(f.payment_amount) AS food_sales,
    SUM(t.payment_amount)/COUNT(DISTINCT(t.account_id)) AS average_account_value,
    SUM(t.discount) AS total_discount,
    SUM(w.sales_amount) AS total_wastage
FROM {{ ref('transactions_clean') }} t 
LEFT JOIN waste w USING (date, time, account_id, type, description, detail, sales_amount)
LEFT JOIN food f USING (date, account_id, description, payment_amount)
WHERE description NOT ILIKE '%ICO%'
GROUP BY hour_of_day
ORDER BY hour_of_day