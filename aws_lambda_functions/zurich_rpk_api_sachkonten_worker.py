import json
import os
import requests
import boto3

# --- Configuration ---
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
API_KEY = os.environ.get('API_KEY')
BASE_URL = "https://api.stadt-zuerich.ch/rpkk-rs/v1"

# --- Global Clients (outside handler for reuse) ---
s3_client = boto3.client('s3')
http_session = requests.Session()
http_session.headers.update({"API-Key": API_KEY, "accept": "application/json"})

# --- Helper function for S3 upload ---
def save_to_s3(data, s3_key):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False)
        )
        print(f"Successfully saved: s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error saving to S3 ({s3_key}): {e}")
        raise # Raise error so SQS retries the message

# --- Main Handler (triggered by SQS) ---
def lambda_handler(event, context):
    """
    Processes a batch of jobs from the SQS queue.
    """
    
    # 'event' contains a batch of SQS messages
    for record in event.get('Records', []):
        try:
            # 1. Read job details from the SQS message
            job_body = record['body']
            job_data = json.loads(job_body)
            
            inst_id = job_data['institution']
            jahr = job_data['jahr']
            betragstyp = job_data['betragstyp']

            print(f"Processing job: Inst={inst_id}, Year={jahr}, Type={betragstyp}...")

            # 2. Assemble API parameters
            query_params = {
                "institution": inst_id,
                "betragsTyp": betragstyp,
                "jahr": [jahr] # According to doc as array
            }
            
            # 3. Define S3 path
            s3_key_dynamisch = f"raw_rpk_api/sachkonto2stellig/institution={inst_id}/jahr={jahr}/betragstyp={betragstyp}/data.json"
            
            # 4. Execute API call
            response = http_session.get(
                f"{BASE_URL}/sachkonto2stellig", 
                params=query_params
            )
            
            # 5. Process response
            if response.status_code == 404:
                print(f"No data (404 Not Found) for job.")
                continue # Job is 'successful' (no data), next message
            
            response.raise_for_status() # Raises error for 5xx, 403 etc.
            data = response.json()
            
            if data.get("value"):
                save_to_s3(data, s3_key_dynamisch)
            else:
                print(f"No data (empty 'value' list) for job.")

        except Exception as e:
            print(f"ERROR processing job: {job_body}. Error: {e}")
            # Raises an error. Lambda will fail.
            # SQS will give this message to another worker
            # for reprocessing after the 'Visibility Timeout'.
            raise e 

    print("Batch processing completed.")
    return {
        'statusCode': 200,
        'body': 'Batch successfully processed.'
    }