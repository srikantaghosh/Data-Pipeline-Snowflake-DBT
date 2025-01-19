

{{
  config(
    materialized = 'table',
    )
}}

WITH summary AS (
SELECT 
    player_id,
    team_id,
    COUNT(DISTINCT match_id) AS total_matches,
    SUM(minutes_played) AS total_minutes_played,
    SUM(total_events) AS total_events,
    SUM(total_qualifiers) AS total_qualifiers,
    SUM(total_variables) AS total_variables
FROM 
    {{ ref('fct_match_positions') }}
LEFT JOIN 
    {{ ref('fct_events') }} USING (match_id, team_id, player_id, position_id)
LEFT JOIN 
    {{ ref('fct_summary') }} USING (match_id, team_id, player_id, position_id)
GROUP BY 
    player_id, team_id
)

SELECT * FROM summary