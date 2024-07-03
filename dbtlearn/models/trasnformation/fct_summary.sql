WITH calculated_summary AS(

    SELECT 
    match_id,
    team_id,
    player_id,
    position_id,
    SUM(variable) AS total_variables
FROM 
    {{ ref('stg_match_summary') }}
GROUP BY match_id, team_id, player_id, position_id
)

SELECT *
FROM calculated_summary