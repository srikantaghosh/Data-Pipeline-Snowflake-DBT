### Airflow DAG that schedules the loading of data from S3 to Snowflake.
## This is a sample code

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import datetime

def load_data_to_snowflake(provider, s3_path, snowflake_table):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    copy_sql = f"""
    COPY INTO {snowflake_table}
    FROM '{s3_path}'
    FILE_FORMAT = (type = 'CSV', field_optionally_enclosed_by = '"')
    PATTERN = '.*.csv'
    ON_ERROR = 'CONTINUE';
    """
    snowflake_hook.run(copy_sql)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'daily_load_s3_to_snowflake',
    default_args=default_args,
    description='Daily load of S3 data to Snowflake',
    schedule_interval='@daily',
)

providers = [
    {'provider': 'provider1', 's3_path': 's3://my-bucket/provider1/soccer_data/', 'snowflake_table': 'provider1_raw'},
    {'provider': 'provider2', 's3_path': 's3://my-bucket/provider2/soccer_data/', 'snowflake_table': 'provider2_raw'},
    {'provider': 'provider3', 's3_path': 's3://my-bucket/provider3/soccer_data/', 'snowflake_table': 'provider3_raw'},
]

for provider in providers:
    task = PythonOperator(
        task_id=f'load_{provider["provider"]}_data',
        python_callable=load_data_to_snowflake,
        op_args=[provider['provider'], provider['s3_path'], provider['snowflake_table']],
        dag=dag,
    )
