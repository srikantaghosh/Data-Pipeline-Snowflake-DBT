WITH club_elo AS(
    SELECT REPLACE(team_id, '"', '') AS team_id ,
    REPLACE(elo_rating, '"', '') AS elo_rating 
    FROM {{ source('soccer', 'club_elo') }}
    
    
)

SELECT * FROM club_elo
