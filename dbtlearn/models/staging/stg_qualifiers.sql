WITH qualifiers AS(
    SELECT REPLACE(match_id, '"', '') AS match_id,
    team_id ,
    player_id ,
    position_id 
    event_id ,
    REPLACE(qualifier_id, '"', '') AS qualifier_id 
    FROM {{ source('soccer', 'qualifiers') }}
)

SELECT * FROM qualifiers