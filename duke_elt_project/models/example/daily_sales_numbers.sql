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
    t.date,
    COUNT(DISTINCT(t.account_id)) AS number_of_accounts,
    SUM(t.payment_amount) AS total_sales,
    SUM(f.payment_amount) AS food_sales,
    SUM(t.payment_amount)/COUNT(DISTINCT(t.account_id)) AS average_account_value,
    SUM(t.discount) AS total_discount,
    SUM(w.sales_amount) AS total_wastage
FROM {{ ref('transactions_clean') }} t 
LEFT JOIN waste w USING (date, time, account_id, type, description, detail, sales_amount)
LEFT JOIN food f USING (date, account_id, description, payment_amount)
GROUP BY t.date
ORDER BY t.date

