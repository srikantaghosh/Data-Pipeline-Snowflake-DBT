{{
  config(
    materialized = 'incremental',
    unique_key = 'payment_id',
    )
}}

-- Incremental Load Logic
With incremental_data AS(
    SELECT *
    FROM {{ ref('stg_payments') }}
    WHERE transaction_ingestion_date > COALESCE(SELECT max(transaction_ingestion_date) FROM {{this}}),
    CURRENT_DATE - INTERVAL '7 DAYS' -- FALLBACK FOR INITIAL RUN
)

-- Late Arriving Data Logic

-- Combine Both data

-- Dedupe-Keep the latest record by ingestion date