-- player_id_table.sql


with provider1 as (
    select
        provider_id as provider_id1,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country
    from {{ source('provider1', 'player_data') }}
),
provider2 as (
    select
        provider_id as provider_id2,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country
    from {{ source('provider2', 'player_data') }}
),
provider3 as (
    select
        provider_id as provider_id3,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country
    from {{ source('provider3', 'player_data') }}
),
combined as (
    select
        coalesce(provider1.player_name, provider2.player_name, provider3.player_name) as name,
        coalesce(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth) as dob,
        provider1.provider_id1,
        provider2.provider_id2,
        provider3.provider_id3,
        row_number() over (partition by coalesce(provider1.player_name, provider2.player_name, provider3.player_name) order by coalesce(provider1.date_of_birth, provider2.date_of_birth, provider3.date_of_birth)) as global_player_id
    from provider1
    full outer join provider2 on provider1.first_name = provider2.first_name and provider1.last_name = provider2.last_name
    full outer join provider3 on provider1.first_name = provider3.first_name and provider1.last_name = provider3.last_name
)

select
    global_player_id,
    name,
    dob,
    provider_id1,
    provider_id2,
    provider_id3
from combined
