{{
    {{
      config(
        materialized = 'view',
        )
    }}
}}

select 
        transaction_id,
        customer_id,
        merchant_id,
        payment_method,
        currency_id,
        amount,
        status, --'SUCCESS', 'FAILED', 'PENDING'
        transaction_timestamp,
        CURRENT_TIMESTAMP() AS transaction_loaded_date -- Timestamp when data was ingested
from {{ source('raw', 'payments') }}
WHERE  status IS NOT NULL