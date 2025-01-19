WITH match_info AS (
    SELECT REPLACE(match_id, '"', '') AS match_id ,
    date::DATE,
    team1_id ,
    team2_id ,
    REPLACE(venue, '"', '') AS venue 
FROM {{ source('soccer', 'match_info') }}
)

SELECT * FROM match_info