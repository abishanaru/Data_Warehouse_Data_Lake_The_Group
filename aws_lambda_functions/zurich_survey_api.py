import boto3
import requests
import os

# Configuration - You can also set these values as environment variables in Lambda
S3_BUCKET_NAME = "zurichcantonexpenditures"  # <-- CHANGE: Your S3 bucket name
S3_KEY = "raw_survey/bevoelkerung_survey.csv"  # <-- CHANGE: The desired file path in S3
DATA_URL = "https://data.stadt-zuerich.ch/dataset/prd_stez_bevoelkerungsbefragungen_seit2019_od4732/download/BEV473OD4732.csv"

# Initialize the S3 client
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    try:
        print(f"Starting download from: {DATA_URL}")
        
        # Stream=True is important for potentially large files to save memory
        with requests.get(DATA_URL, stream=True) as r:
            r.raise_for_status()  # Raises an error if the download fails (e.g., 404)
            
            # Upload data directly from the stream to S3
            # upload_fileobj is the most memory-efficient method
            s3_client.upload_fileobj(r.raw, S3_BUCKET_NAME, S3_KEY)
            
        print(f"Successfully downloaded and saved to s3://{S3_BUCKET_NAME}/{S3_KEY}.")
        
        return {
            'statusCode': 200,
            'body': f'File successfully uploaded to s3://{S3_BUCKET_NAME}/{S3_KEY}.'
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        raise e  # Mark Lambda as failed
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise eimport boto3
import requests
import os

# Configuration 
S3_BUCKET_NAME = "zurichcantonexpenditures" 
S3_KEY = "raw_survey/bevoelkerung_survey.csv" 
DATA_URL = "https://data.stadt-zuerich.ch/dataset/prd_stez_bevoelkerungsbefragungen_seit2019_od4732/download/BEV473OD4732.csv"

# Initialize the S3 client
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    try:
        print(f"Starting download from: {DATA_URL}")
        
        # Stream=True is important for potentially large files to save memory
        with requests.get(DATA_URL, stream=True) as r:
            r.raise_for_status()  # Raises an error if the download fails (e.g., 404)
            
            # Upload data directly from the stream to S3
            # upload_fileobj is the most memory-efficient method
            s3_client.upload_fileobj(r.raw, S3_BUCKET_NAME, S3_KEY)
            
        print(f"Successfully downloaded and saved to s3://{S3_BUCKET_NAME}/{S3_KEY}.")
        
        return {
            'statusCode': 200,
            'body': f'File successfully uploaded to s3://{S3_BUCKET_NAME}/{S3_KEY}.'
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        raise e  # Mark Lambda as failed
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e