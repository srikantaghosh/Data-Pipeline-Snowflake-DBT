{{
  config(
    materialized = 'table',
    )
}}

WITH position AS (

    SELECT * FROM {{ ref('stg_position') }}
)

SELECT * FROM position