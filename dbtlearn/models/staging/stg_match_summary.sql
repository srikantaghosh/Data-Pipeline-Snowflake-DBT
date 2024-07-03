WITH match_summary AS(
    SELECT REPLACE(match_id, '"', '') AS match_id,
    team_id ,
    player_id ,
    position_id ,
    REPLACE(variable, '"', '') AS variable 
    FROM {{ source('soccer', 'match_summary') }}
)

SELECT * FROM match_summary