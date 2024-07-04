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

def main():
    # fetching data by club name
    club_name = 'realmadrid'
    data = fetch_club_elo(club_name=club_name)
     
    bucket_name = 'apc-data-engineering' 
    object_name = f'club_elo_{club_name}.csv'  # Naming convention for storing in S3
    upload_to_s3(data, bucket_name, object_name)

if __name__ == "__main__":
    main()
