WITH lineups AS(
    SELECT REPLACE(match_id, '"', '') AS match_id,
    team_id ,
    player_id ,
    REPLACE(position_id, '"', '') AS position_id 
    FROM {{ source('soccer', 'lineups') }}
)

SELECT * FROM lineups