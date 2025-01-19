WITH provider_data AS (
    SELECT 
        COALESCE(provider1.first_name, provider2.first_name, provider3.first_name) AS first_name,
        COALESCE(provider1.last_name, provider2.last_name, provider3.last_name) AS last_name,
        COALESCE(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) AS date_of_birth,
        COALESCE(provider1.player_name, provider2.player_name, provider3.player_name) AS name,
        COALESCE(provider1.provider_id, provider2.provider_id, provider3.provider_id) AS final_provider_id,
        provider1.provider_id AS provider_id1,
        provider2.provider_id AS provider_id2,
        provider3.provider_id AS provider_id3
    FROM 
        {{ ref('stg_provider1') }} provider1 
    FULL OUTER JOIN 
        {{ ref('stg_provider2') }} provider2 
    ON 
        provider1.first_name = provider2.first_name 
        AND provider1.last_name = provider2.last_name
        AND provider1.date_of_birth = provider2.date_of_birth
    FULL OUTER JOIN 
        {{ ref('stg_provider3') }} provider3 
    ON 
        COALESCE(provider1.first_name, provider2.first_name) = provider3.first_name 
        AND COALESCE(provider1.last_name, provider2.last_name) = provider3.last_name
        AND COALESCE(provider1.date_of_birth, provider2.date_of_birth) = provider3.date_of_birth
),
deduplicated_provider_data AS (
    SELECT DISTINCT
        first_name,
        last_name,
        date_of_birth,
        name,
        provider_id1,
        provider_id2,
        provider_id3
    FROM provider_data
),
provider_with_id AS (
    SELECT
        name,
        date_of_birth AS dob,
        provider_id1,
        provider_id2,
        provider_id3,
        ROW_NUMBER() OVER (
            ORDER BY first_name, last_name, date_of_birth
        ) AS global_player_id
    FROM deduplicated_provider_data
)
, provider_final AS (
    SELECT
        global_player_id,
        name,
        dob,
        provider_id1,
        provider_id2,
        provider_id3
    FROM provider_with_id
    ORDER BY global_player_id
)

SELECT 
    global_player_id,
    name,
    dob,
    COALESCE(provider_id1, provider_id2, provider_id3) AS final_provider_id
FROM 
    provider_final
QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY global_player_id) = 1
