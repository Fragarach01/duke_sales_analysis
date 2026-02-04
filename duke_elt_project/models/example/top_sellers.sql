{{ config(materialized='view') }}

SELECT
    description,
    COUNT(description) as number_of_sales,
    SUM(sales_amount) as total_sales
FROM {{ ref('transactions_clean') }}
WHERE type = 'Sale' and description NOT ILIKE '%ICO%'
GROUP BY description
ORDER BY number_of_sales DESC