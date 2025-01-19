{{
  config(
    materialized = 'views'
    )
}}

SELECT customer_id
    , customer_name
    , cnt_users
    , remediation_action
FROM {{ ref('seed_user_events') }}
