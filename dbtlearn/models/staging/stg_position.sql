
WITH player_position AS (
   SELECT position_id,
    position_name
    FROM {{ source('soccer', 'player_position') }}
)

SELECT * FROM player_position
