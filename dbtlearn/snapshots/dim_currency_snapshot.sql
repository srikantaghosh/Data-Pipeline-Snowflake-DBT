{% {% snapshot dim_currency_snapshot %}

{{
   config(
       target_database='staging', 
       target_schema='snapshots',
       unique_key='currency_id',

       strategy='timestamp',
       updated_at='updated_at',
   )
}}

SELECT 
    currency_id,
    currency_code,
    exchange_rate_to_pound,
    updated_at
FROM {{ source('raw', 'raw_currency') }}

{% endsnapshot %}%}