WITH events AS (
    SELECT REPLACE(match_id, '"', '') AS match_id ,
    team_id ,
    player_id ,
    position_id ,
    REPLACE(event_id, '"', '') AS event_id 
    FROM {{ source('soccer', 'events') }}
)

SELECT * FROM events