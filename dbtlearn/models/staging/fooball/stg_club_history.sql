WITH club AS(
SELECT
    Rank ,
        Club ,
        Country ,
        Level ,
        Elo,
        "From",
        "To"
    FROM {{ source('soccer', 'club_history') }}
)

SELECT * FROM club