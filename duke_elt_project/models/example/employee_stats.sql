{{ config(materialized='view') }}


SELECT 
    employee,
    COUNT(DISTINCT account_id) AS number_of_accounts,
    SUM(sales_amount) AS total_sales,
    SUM(sales_amount)/COUNT(DISTINCT account_id) AS average_account_value
FROM {{ ref('transactions_clean') }}
GROUP BY employee
ORDER BY total_sales DESC

