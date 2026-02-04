{{ config(materialized='view') }}

WITH dow AS (
    SELECT 
        *,
        EXTRACT(DOW FROM date) AS day_of_week
    FROM {{ ref('daily_sales_numbers') }}
)

SELECT 
    CASE 
        WHEN day_of_week = 0 THEN 'Sunday'
        WHEN day_of_week = 1 THEN 'Monday'
        WHEN day_of_week = 2 THEN 'Tuesday'
        WHEN day_of_week = 3 THEN 'Wednesday'
        WHEN day_of_week = 4 THEN 'Thursday'
        WHEN day_of_week = 5 THEN 'Friday'
        WHEN day_of_week = 6 THEN 'Saturday'
    END AS day_of_week,
    AVG(number_of_accounts) as avg_number_of_accounts,
    AVG(total_sales) as avg_sales,
    AVG(food_sales) as avg_food_sales,
    AVG(average_account_value) as avg_account_value,
    AVG(total_discount) as avg_total_discounts,
    AVG(total_wastage) as avg_total_wastage
FROM dow
GROUP BY day_of_week