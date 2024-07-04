-- All credentials have been masked due to security

--sql
-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='xxx'
  LOGIN_NAME='xxx'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='SOCCER.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS SOCCER;
CREATE SCHEMA IF NOT EXISTS SOCCER.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE SOCCER to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE SOCCER to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE SOCCER to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA SOCCER.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA SOCCER.RAW to ROLE transform;

```

## Snowflake data import



```sql
-- Set up the defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE soccer;
USE SCHEMA RAW;


CREATE STORAGE INTEGRATION sf_s3_intg
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'xxx' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://apc-data-engineering/')
  ;

  
  
DESCRIBE INTEGRATION sf_s3_intg;
CREATE STAGE snowflake_stage
  STORAGE_INTEGRATION = sf_s3_intg
  URL = 's3://apc-data-engineering'
;

LIST @snowflake_stage;

-- Create our three tables and import the data from S3
-- Create table for provider1 raw data
CREATE OR REPLACE TABLE provider1_raw (
    provider_id STRING,
    player_name STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    country STRING,
    nickname STRING,
    jersey_number STRING,
    nationality STRING
);


                    
COPY INTO provider1_raw (provider_id,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country,
        nickname,
        jersey_number,
        nationality)
                   from 's3://apc-data-engineering/provider1 -Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    
                    );

CREATE OR REPLACE TABLE provider2_raw (
    provider_id STRING,
    player_name STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    country STRING,
    nickname STRING,
    jersey_number STRING,
    nationality STRING
);


                    
COPY INTO provider2_raw (provider_id,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country,
        nickname,
        jersey_number,
        nationality)
                   from 's3://apc-data-engineering/provider2 -Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    
                    );


                   

CREATE OR REPLACE TABLE provider3_raw (
    provider_id STRING,
    player_name STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    country STRING,
    nickname STRING,
    jersey_number STRING,
    nationality STRING
);


                    
COPY INTO provider3_raw (provider_id,
        player_name,
        first_name,
        last_name,
        date_of_birth,
        gender,
        country,
        nickname,
        jersey_number,
        nationality)
                   from 's3://apc-data-engineering/provider3 - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    
                    );
-- Loading CLUB ELO data

CREATE OR REPLACE TABLE club_elo (
    team_id STRING,
    elo_rating STRING
);

COPY INTO club_elo (
        team_id,
        elo_rating)
                   from 's3://apc-data-engineering/club_elo - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );
                    
CREATE OR REPLACE TABLE match_info (
    match_id STRING,
    date DATE,
    team1_id STRING,
    team2_id STRING,
    venue STRING
);

COPY INTO match_info (
        match_id,
        date ,
        team1_id ,
        team2_id ,
        venue )
                   from 's3://apc-data-engineering/match_info - Sheet1.cs'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );

CREATE OR REPLACE TABLE lineups (
    match_id STRING,
    team_id STRING,
    player_id STRING,
    position_id STRING
);

COPY INTO lineups (
   match_id ,
    team_id ,
    player_id ,
    position_id )
                   from 's3://apc-data-engineering/lineups - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );

CREATE OR REPLACE TABLE events (
    match_id STRING,
    team_id STRING,
    player_id STRING,
    position_id STRING,
    event_id STRING
);

COPY INTO events (
    match_id ,
    team_id ,
    player_id ,
    position_id ,
    event_id )
                   from 's3://apc-data-engineering/events - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );

CREATE OR REPLACE TABLE qualifiers (
    match_id STRING,
    team_id STRING,
    player_id STRING,
    position_id STRING,
    event_id STRING,
    qualifier_id STRING
);
COPY INTO qualifiers (
    match_id ,
    team_id ,
    player_id ,
    position_id ,
    event_id ,
    qualifier_id )
                   from 's3://apc-data-engineering/qualifiers - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );

CREATE OR REPLACE TABLE match_summary (
    match_id STRING,
    team_id STRING,
    player_id STRING,
    position_id STRING,
    variable STRING
);
COPY INTO match_summary (
        match_id ,
        team_id ,
        player_id ,
        position_id ,
        variable )
                   from 's3://apc-data-engineering/match_summary - Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );


-- club elo api

CREATE OR REPLACE TABLE club_history (
    Rank STRING,
    Club STRING,
    Country STRING,
    Level STRING,
    Elo FLOAT,
    "From" DATE,
    "To" DATE
);

COPY INTO club_history (
        Rank ,
        Club ,
        Country ,
        Level ,
        Elo,
        "From",
        "To"
        )
                   from 's3://apc-data-engineering/club_elo_realmadrid.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );

CREATE OR REPLACE TABLE day_level_club_elo_rank (
    Rank STRING,
    Club STRING,
    Country STRING,
    Level STRING,
    Elo FLOAT,
    "From" DATE,
    "To" DATE
);

COPY INTO day_level_club_elo_rank (
        Rank ,
        Club ,
        Country ,
        Level ,
        Elo,
        "From",
        "To"
        )
                   from 's3://apc-data-engineering/club_elo_2024-07-04.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );
                    


