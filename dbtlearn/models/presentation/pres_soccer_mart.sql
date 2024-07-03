WITH player AS(


SELECT 
    DISTINCT GLOBAL_PLAYER_ID
,NAME
,DOB
,PROVIDER_ID1
,PROVIDER_ID2
,PROVIDER_ID3

,TOTAL_MATCHES
,TOTAL_MINUTES_PLAYED
,TOTAL_EVENTS
,TOTAL_QUALIFIERS
,TOTAL_VARIABLES
,MATCH_ID
,SS.TEAM_ID
,POSITION_ID
,MINUTES_PLAYED
FROM {{ ref('fct_player') }} fp
LEFT JOIN 
   {{ ref('fct_season_summary') }}  ss ON fp.global_player_id::VARCHAR = ss.player_id::VARCHAR 
LEFT JOIN 
    {{ ref('fct_match_positions') }}  mp ON fp.global_player_id::VARCHAR  = mp.player_id::VARCHAR 
)

SELECT * FROM player