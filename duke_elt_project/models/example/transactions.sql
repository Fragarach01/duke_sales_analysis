{{ config(materialized='table') }}

select 
    date::DATE,
    time::TIME, 
    order_no::NUMERIC, 
    CASE 
        WHEN account_no IN ('""', NULL) THEN 1 
        ELSE account_no::INTEGER 
    END AS account_no,  
    account_id::NUMERIC, 
    type, 
    description, 
    detail, 
    information, 
    CASE 
        WHEN quantity IN ('""', NULL) THEN 1 
        ELSE quantity::INTEGER 
    END AS quantity, 
    sales_amount::FLOAT, 
    discount::FLOAT, 
    VAT::FLOAT, 
    payment_amount::FLOAT, 
    change::FLOAT
from {{ source('target_db', 'transactions') }}
