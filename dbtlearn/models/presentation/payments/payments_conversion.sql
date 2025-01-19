{{
  config(
    materialized='table' -- Final table for reporting
  )
}}

WITH payments AS (
    SELECT 
        payment_id,
        merchant_id,
        amount AS original_amount,
        currency_id,
        transaction_date,
        transaction_status
    FROM {{ ref('intermediate_payments') }}
),

currency_rates AS (
    SELECT
        currency_id,
        exchange_rate_to_pound,
        dbt_valid_from,
        dbt_valid_to COALESCE(dbt_valid_to, '9999-12-31') AS dbt_valid_to
    FROM {{ ref('dim_currency_snapshot') }}
),

-- Point-in-Time Join to Get Currency Conversion Rate
joined_data AS (
    SELECT
        p.payment_id,
        p.merchant_id,
        p.original_amount,
        c.exchange_rate_to_pound,
        p.original_amount * c.exchange_rate_to_pound AS converted_amount,
        p.currency_id,
        p.transaction_date,
        p.transaction_status
    FROM payments p
    LEFT JOIN currency_rates c
    ON p.currency_id = c.currency_id
    AND p.transaction_date BETWEEN c.dbt_valid_from AND c.dbt_valid_to
)

SELECT 
    payment_id,
    merchant_id,
    original_amount,
    currency_id,
    exchange_rate_to_pound,
    converted_amount,
    transaction_date,
    transaction_status
FROM joined_data;
