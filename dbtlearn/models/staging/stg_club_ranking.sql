WITH rank AS(
SELECT
    Rank ,
        Club ,
        Country ,
        Level ,
        Elo,
        "From",
        "To"
    FROM {{ source('soccer', 'day_level_club_elo_rank') }}
)

SELECT * FROM rank