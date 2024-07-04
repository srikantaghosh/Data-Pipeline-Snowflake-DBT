{{
  config(
    materialized = 'table',
    )
}}

WITH match_positions AS (

    SELECT 
    match_id,
    team_id,
    player_id,
    position_id,
    90 AS minutes_played  -- Assuming each player plays the full 90 minutes in their position
FROM 
    {{ ref('stg_lineups') }}
)

SELECT * FROM match_positions