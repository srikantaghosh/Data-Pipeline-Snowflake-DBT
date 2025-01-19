{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

-- Incremental Load Logic
WITH incremental_data AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
    WHERE transaction_ingestion_date > COALESCE(
        (SELECT MAX(transaction_ingestion_date) FROM {{ this }}), -- Use ingestion date for watermark
        CURRENT_DATE - INTERVAL '7 DAYS'                         -- Fallback for initial run
    )
),

-- Late-Arriving Data Logic
late_arrivals AS (
    SELECT *
    FROM {{ ref('stg_payments') }}
    WHERE transaction_date < (SELECT MAX(transaction_date) FROM {{ this }}) -- Late transaction logic
    AND transaction_ingestion_date > (SELECT MAX(transaction_ingestion_date) FROM {{ this }})
)

-- Combine Incremental and Late-Arriving Data
,combined(
SELECT * FROM incremental_data

UNION ALL

SELECT * FROM late_arrivals)

SELECT payment_id,
    transaction_date,
    user_id,
    merchant_id,
    currency_id,
    amount,
    transaction_status,
    transaction_ingestion_date,
    updated_at
FROM 
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY transaction_ingestion_date DESC)=1 ---- Keep the latest record by ingestion date
