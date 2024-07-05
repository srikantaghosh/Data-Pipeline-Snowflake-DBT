

{{
  config(
    materialized = 'table',
    )
}}

WITH provider_data AS (
    SELECT 
        provider1.first_name,
        provider1.last_name,
        provider1.date_of_birth,
        COALESCE(provider1.player_name, provider2.player_name, provider3.player_name) AS name,
        COALESCE(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) AS dob,
        provider1.provider_id AS provider_id1,
        provider2.provider_id AS provider_id2,
        provider3.provider_id AS provider_id3
    FROM 
        {{ ref('stg_provider1') }} provider1 
    FULL OUTER JOIN 
        {{ ref('stg_provider2') }}  provider2 
    ON 
        provider1.first_name = provider2.first_name 
        AND provider1.last_name = provider2.last_name
        AND provider1.date_of_birth = provider2.date_of_birth
    FULL OUTER JOIN 
        {{ ref('stg_provider3') }} provider3 
    ON 
        provider1.first_name = provider3.first_name 
        AND provider1.last_name = provider3.last_name
        AND provider1.date_of_birth = provider3.date_of_birth
    UNION
    SELECT 
        provider2.first_name,
        provider2.last_name,
        provider2.date_of_birth,
        COALESCE(provider1.player_name, provider2.player_name, provider3.player_name) AS name,
        COALESCE(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) AS dob,
        provider1.provider_id AS provider_id1,
        provider2.provider_id AS provider_id2,
        provider3.provider_id AS provider_id3
    FROM 
        {{ ref('stg_provider2') }}  provider2 
    LEFT JOIN 
        {{ ref('stg_provider1') }} provider1 
    ON 
        provider1.first_name = provider2.first_name 
        AND provider1.last_name = provider2.last_name
        AND provider1.date_of_birth = provider2.date_of_birth
    LEFT JOIN 
        SOCCER.DEV.stg_provider3 provider3 
    ON 
        provider2.first_name = provider3.first_name 
        AND provider2.last_name = provider3.last_name
        AND provider2.date_of_birth = provider3.date_of_birth
    UNION
    SELECT 
        provider3.first_name,
        provider3.last_name,
        provider3.date_of_birth,
        COALESCE(provider1.player_name, provider2.player_name, provider3.player_name) AS name,
        COALESCE(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) AS dob,
        provider1.provider_id AS provider_id1,
        provider2.provider_id AS provider_id2,
        provider3.provider_id AS provider_id3
    FROM 
        {{ ref('stg_provider3') }} provider3 
    LEFT JOIN 
        {{ ref('stg_provider1') }} provider1 
    ON 
        provider1.first_name = provider3.first_name 
        AND provider1.last_name = provider3.last_name
        AND provider1.date_of_birth = provider3.date_of_birth
    LEFT JOIN 
        {{ ref('stg_provider2') }}  provider2 
    ON 
        provider2.first_name = provider3.first_name 
        AND provider2.last_name = provider3.last_name
        AND provider2.date_of_birth = provider3.date_of_birth
),
deduplicated_provider_data AS (
    SELECT DISTINCT
        first_name,
        last_name,
        date_of_birth,
        name,
        dob,
        provider_id1,
        provider_id2,
        provider_id3
    FROM provider_data
),
provider_with_id AS (
    SELECT
        name,
        dob,
        provider_id1,
        provider_id2,
        provider_id3,
        ROW_NUMBER() OVER (
            ORDER BY first_name, last_name, date_of_birth
        ) AS global_player_id
    FROM deduplicated_provider_data
)

SELECT
    global_player_id,
    name,
    dob,
    provider_id1,
    provider_id2,
    provider_id3
FROM provider_with_id
ORDER BY global_player_id
