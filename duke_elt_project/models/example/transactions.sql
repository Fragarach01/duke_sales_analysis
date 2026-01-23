{{ config(materialized='table') }}

select 
    date::DATE,
    time::TIME,
    account_id::NUMERIC, 
    terminal,
    type, 
    description, 
    detail, 
    information, 
    employee,
    CASE 
        WHEN quantity IN ('""', NULL) THEN 1 
        ELSE quantity::INTEGER 
    END AS quantity, 
    sales_amount::FLOAT, 
    discount::FLOAT, 
    payment_amount::FLOAT, 
    change::FLOAT
from {{ source('target_db', 'transactions') }}
