{{
  config(
    materialized = 'table',
    )
}}

WITH provider AS (
Select
        coalesce(provider1.player_name, provider2.player_name, provider3.player_name) as name,
        coalesce(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) as dob,
        provider1.PROVIDER_ID AS provider_id1,
        provider2.PROVIDER_ID AS provider_id2,
        provider3.PROVIDER_ID AS provider_id3,
        row_number() over (partition by coalesce(provider1.player_name, provider2.player_name, provider3.player_name) order by coalesce(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth)) as global_player_id
    from {{ ref('stg_provider1') }} provider1 
    full outer join {{ ref('stg_provider2') }} provider2 on provider1.first_name = provider2.first_name and provider1.last_name = provider2.last_name
    full outer join {{ ref('stg_provider3') }} provider3 on provider1.first_name = provider3.first_name and provider1.last_name = provider3.last_name
    )

select
    global_player_id,
    name,
    dob,
    provider_id1,
    provider_id2,
    provider_id3
from provider