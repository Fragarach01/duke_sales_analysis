{{ config(materialized='table') }}

select *
from {{ ref('transactions') }}
where type = 'Payment'