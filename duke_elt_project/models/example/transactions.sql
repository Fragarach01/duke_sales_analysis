{{ config(materialized='table') }}

select 
    date::DATE,
    time::TIME,
    CASE 
        WHEN account_id IN ('""', NULL) THEN 0 
        ELSE account_id::NUMERIC 
    END AS account_id,
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
