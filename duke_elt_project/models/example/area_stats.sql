{{ config(materialized='view') }}

SELECT 
    terminal,
    COUNT(DISTINCT account_id) AS number_of_accounts,
    SUM(payment_amount) AS total_sales
FROM {{ ref('transactions_clean') }}
GROUP BY terminal
ORDER BY total_sales DESC