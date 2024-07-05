from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests

def fetch_and_dump_data_to_s3(url, s3_bucket, s3_key):
    response = requests.get(url) #Makes a GET request to the provided URL.
    data = response.text #Retrieves the response content as a text string (CSV data).
    
    s3 = S3Hook(aws_conn_id='my_aws_conn') #Initialise the connection
    # Uploads the CSV data as a string to the specified S3 bucket and key.
    s3.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    ) 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('fetch_and_dump_data', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    tasks = []
    
    urls = [
        'https://api.provider1.com/data',
        'https://api.provider2.com/data',
        'https://api.provider3.com/data',
        'https://api.clubelo.com/YYYY-MM-DD'
    ]
    
    s3_bucket = 'apc-data-engineering'
    ## We can create separate dags for each url in prod. 
    for i, url in enumerate(urls):
        task = PythonOperator(
            task_id=f'fetch_and_dump_{i}',
            python_callable=fetch_and_dump_data_to_s3,
            op_args=[url, s3_bucket, f'data/provider_{i+1}.csv'],
        )
        tasks.append(task)

    tasks
