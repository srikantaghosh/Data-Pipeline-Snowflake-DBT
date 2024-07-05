--This contains sql queries, which will answer all the business questions
/*
• Minutes a player played in each position
• Position specific calculations of player stats
• Player season summaries
• Player match summaries
• Squads selected
*/

--Minutes a player played in each position
SELECT b.global_player_id,
    b.name,
    position_id,
    SUM(minutes_played) AS total_minutes_played
FROM 
    SOCCER.DEV.FCT_MATCH_POSITIONS a
    JOIN SOCCER.DEV.FCT_PLAYER b
    on a.player_id =  b.final_provider_id
GROUP BY 
    1,2,3;


--Position-specific calculations of player stats
    
SELECT 
    player_id,
    position_id,
    SUM(total_events) AS total_events,
    SUM(total_qualifiers) AS total_qualifiers,
    SUM(total_variables) AS total_variables
FROM 
    SOCCER.DEV.FCT_MATCH_POSITIONS
LEFT JOIN 
    SOCCER.DEV.FCT_EVENTS 
    USING (match_id, team_id, player_id, position_id)
LEFT JOIN 
    SOCCER.DEV.FCT_SUMMARY 
    USING (match_id, team_id, player_id, position_id)
GROUP BY 
    player_id, position_id;

--Player season summaries
SELECT 
    player_id,
    team_id,
    total_matches,
    total_minutes_played,
    total_events,
    total_qualifiers,
    total_variables
FROM 
    SOCCER.DEV.FCT_SEASON_SUMMARY;

--Player match summaries
SELECT 
    player_id,
    match_id,
    SUM(total_events) AS total_events,
    SUM(total_qualifiers) AS total_qualifiers,
    SUM(total_variables) AS total_variables
FROM 
    SOCCER.DEV.FCT_EVENTS
LEFT JOIN 
    SOCCER.DEV.FCT_SUMMARY  USING (match_id, team_id, player_id, position_id)
GROUP BY 
    player_id, match_id;


--Squads selected
SELECT 
    match_id,
    team_id,
    ARRAY_AGG(player_id) AS squad
FROM 
    SOCCER.DEV.STG_LINEUPS
GROUP BY 
    match_id, team_id;
