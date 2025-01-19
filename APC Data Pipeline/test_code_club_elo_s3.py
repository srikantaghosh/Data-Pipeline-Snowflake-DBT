# TEST CODE 
# LOADING CLUB ELO DATA TO S3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def fetch_club_elo(club_name):
    base_url = 'http://api.clubelo.com/'
    url = f"{base_url}{club_name}"

    print(f"Fetching data from URL: {url}")
    response = requests.get(url)
    
    try:
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.text  # Read the CSV data as text
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        raise
    
    return data

def create_bucket_if_not_exists(bucket_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except s3_client.exceptions.NoSuchBucket:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created.")

def upload_to_s3(data, bucket_name, object_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=data,
            ContentType='text/csv'
        )
        print("Upload Successful")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"An error occurred: {e}")

def run_etl():
    club_name = 'realmadrid'
    data = fetch_club_elo(club_name=club_name)
    
    bucket_name = 'my-bucket'  # Replace with your bucket name
    create_bucket_if_not_exists(bucket_name)  # Ensure the bucket exists
    
    object_name = f'club_elo_{club_name}.csv'  # The name of the object to be stored in S3
    upload_to_s3(data, bucket_name, object_name)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('fetch_club_elo_dag', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload_club_elo',
        python_callable=run_etl
    )

    fetch_and_upload_task
