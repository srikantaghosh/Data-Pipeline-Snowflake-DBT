WITH prov AS (
SELECT REPLACE(provider_id, '"', '') AS provider_id,
    player_name,
    first_name,
    last_name,
    date_of_birth,
    gender,
    country,
    nickname,
    jersey_number,
    REPLACE(nationality, '"', '') AS nationality
FROM {{ source('soccer', 'provider3_raw') }}
)

SELECT * FROM prov