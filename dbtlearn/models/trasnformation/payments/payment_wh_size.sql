
{{ config(
    pre_hook=[
        "ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'XLARGE'"
    ],
    post_hook=[
        "ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'SMALL'"
    ]
) }}

SELECT 
    transaction_id,
    customer_id,
    SUM(amount) AS total_amount
FROM {{ ref('fact_transactions') }}
GROUP BY transaction_id, customer_id;

{{
  config(
    materialized = 'incremental',
    pre_hook = ["ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'LARGE'"],
    post_hook = ["ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'XSMALL'"]
    )
}}
