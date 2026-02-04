{{ config(materialized='table') }}


WITH transactions AS (
    SELECT 
        TO_DATE(date, 'DD/MM/YYYY') AS date,
        time::TIME,
        CASE 
            WHEN account_id IN ('""', '', NULL) THEN '0' 
            ELSE account_id
        END AS account_id,
        terminal,
        REPLACE(REPLACE(type, N'Void ', N'Clear'), N'Item Not Sold', N'Clear') AS type, 
        description, 
        detail, 
        employee,
        CASE 
            WHEN quantity IN ('""', NULL) THEN 1 
            ELSE quantity::INTEGER 
        END AS quantity, 
        REPLACE(sales_amount, ',', '')::FLOAT AS sales_amount, 
        discount::FLOAT, 
        REPLACE(payment_amount, ',', '')::FLOAT AS payment_amount, 
        change::FLOAT,
        row_number() OVER (PARTITION BY (account_id, description, detail, type) ORDER BY time) as rn
    FROM {{ source('source_db', 'transactions') }}
    WHERE terminal != 'External API 6'
    AND type NOT IN ('No Sale')
    AND description NOT LIKE '%Glass%'
),

-- select account IDS that have clears/voids
accounts_with_clears as (
    SELECT DISTINCT(account_id)
    FROM transactions
    WHERE type = 'Clear'),

-- rank entries of products in accounts that have clears/voids
ranked_entries as (
    SELECT *
--        row_number() OVER (PARTITION BY (account_id, description, detail, type) ORDER BY time) as rn
    FROM transactions
     WHERE account_id = '457396838718468'
--    JOIN accounts_with_clears USING (account_id)
),

-- select ranked clears/voids
ranked_clears_and_voids as (
    SELECT *
    FROM transactions
    WHERE type = 'Clear'
),

-- match clears and voids to the product instances
matches as (
    SELECT rpe.*
    FROM transactions as rpe
    JOIN ranked_clears_and_voids USING (account_id, description, detail, rn)
    ORDER BY account_id, rpe.time
)


--remove clears 
SELECT 
    date,
    time,
    account_id,
    terminal,
    type,
    description,
    detail,
    employee,
    quantity,
    sales_amount,
    discount,
    payment_amount,
    change
FROM transactions 
WHERE NOT EXISTS (
    SELECT 1
    FROM matches m
      WHERE transactions.account_id = m.account_id
      AND transactions.type = m.type
      AND transactions.description = m.description
      AND transactions.detail = m.detail
      AND transactions.quantity = m.quantity
      AND transactions.rn = m.rn
)

/* SELECT * 
FROM transactions t
LEFT JOIN matches USING (date, time, account_id, terminal, type, description, detail, information, employee, quantity, sales_amount, payment_amount, discount, change, rn)
 */

