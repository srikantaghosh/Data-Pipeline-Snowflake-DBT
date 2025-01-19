{{
  config(
    materialized = 'table',
    )
}}

WITH latest_rank AS (
    SELECT 
        Club, 
        MAX("To") AS latest_date
    FROM 
       {{ ref('stg_club_ranking') }}
    GROUP BY 
        Club
)
SELECT 
    d.Rank, 
    d.Club, 
    d.Country, 
    d.Level, 
    d.Elo, 
    d."From", 
    d."To"
FROM 
    {{ ref('stg_club_ranking') }} d
JOIN 
    latest_rank l
ON 
    d.Club = l.Club AND d."To" = l.latest_date