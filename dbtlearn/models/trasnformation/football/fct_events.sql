{{
  config(
    materialized = 'table',
    )
}}

WITH calculated_events AS (
SELECT 
    match_id,
    team_id,
    player_id,
    position_id,
    COUNT(event_id) AS total_events,
    COUNT(DISTINCT qualifier_id) AS total_qualifiers
FROM 
    {{ ref('stg_events') }}
LEFT JOIN 
    SOCCER.DEV.STG_QUALIFIERS USING (match_id, team_id, player_id, event_id)
GROUP BY 
    match_id, team_id, player_id, position_id
)

SELECT * 
FROM calculated_events