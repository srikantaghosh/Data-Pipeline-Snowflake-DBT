{{
  config(
    materialized = 'table',
    )
}}

WITH match AS (

    SELECT * FROM {{ ref('stg_match_info') }}
)

SELECT * FROM match