# Soccer & Athlete Performance Analysis: Cloud Data Pipeline 
Data Pipeline using S3, Snowflake, dbt, Airflow, Python. 

### Services Used:
**The project utilizes the following services:**

1. Samples CSVs to mock Players Data, Club Elo API
2. AWS IAM 
3. AWS S3 (Simple Storage Service) - Data Lake
4. Snowflake Data Warehouse
5. dbt
6. Airflow - WIP

### Architecture
![Architecture](https://github.com/srikantaghosh/Data-Pipeline-Snowflake-DBT/blob/main/DATA%20PIPELINE.png)


### Objective: Building a Robust Data Architecture for Soccer Clubs The architecture should be able to cover the following requirements:
1. Design a centralized sports data platform hosted in the cloud.
2. Integrated data from various internal and external sources centered around soccer teams & athletes.
3. Design the presentation/serving layer that enables stakeholders to utilize the data to make data driven decisions.
4. Snowflake Data Warehouse



### Analytic Key use cases to cover:
• Minutes a player played in each position
• Position specific calculations of player stats
• Player season summaries
• Player match summaries
• Squads selected


### Requirements:
Designing a cloud architecture considering business requirements, which include components from the following layers:
1. Ingestion
2. Storage
3. Transformation
4. Scheduling
5. Serving

NOTE: Sample codes for Data Ingestions, Demo DAGs and Snowflake Queries are stored in this repo. 

For dbt model, please visit- dbtlearn-models


