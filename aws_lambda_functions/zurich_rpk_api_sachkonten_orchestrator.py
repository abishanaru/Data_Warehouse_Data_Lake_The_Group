import json
import os
import requests
import boto3
import datetime

# --- Configuration ---
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
API_KEY = os.environ.get('API_KEY')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL') 
BASE_URL = "https://api.stadt-zuerich.ch/rpkk-rs/v1"

# --- Global Clients ---
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
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

# --- Main Handler ---
def lambda_handler(event, context):
    """
    Loads dimensions and populates the SQS queue with jobs.
    Supports {daily: true} to load only the current year.
    """
    
    # --- 1. Dimension table: Institutions ---
    print("Fetching institutions...")
    institution_ids = []
    try:
        response = http_session.get(f"{BASE_URL}/institutionen")
        response.raise_for_status() 
        institutionen = response.json()
        save_to_s3(institutionen, "raw_rpk_api/institutionen/institutionen.json")
        
        institution_ids = [inst['key'] for inst in institutionen.get('value', [])]
        if not institution_ids:
            print("WARNING: No institution IDs found.")
            return {'statusCode': 500, 'body': 'No institutions found'}
            
    except Exception as e:
        print(f"ERROR fetching institutions: {e}")
        return {"statusCode": 500, "body": f"Error institutions: {e}"}

    # --- 2. Dimension table: Accounts ---
    print("Fetching account definitions...")
    try:
        response = http_session.get(f"{BASE_URL}/konten")
        response.raise_for_status()
        konten = response.json()
        save_to_s3(konten, "raw_rpk_api/konten/konten.json")
    except Exception as e:
        print(f"Error fetching accounts: {e}")
        pass

    # --- 3. Job creation and SQS dispatch ---
    print("Starting job creation for SQS...")
    
    current_year = datetime.datetime.now().year
    
    # --- Logic for years ---
    if event.get('daily'):
        print(f"Mode: DAILY. Creating jobs only for current year: {current_year}")
        jahre_liste = [current_year]
    else:
        # Standard: Load historically
        start_jahr = int(event.get('start_year', 2019))
        end_jahr = int(event.get('end_year', current_year))
        print(f"Mode: FULL/RANGE. Creating jobs from {start_jahr} to {end_jahr}")
        jahre_liste = list(range(start_jahr, end_jahr + 1))

    betragstypen_liste = ["GEMEINDERAT_BESCHLUSS", "RECHNUNG", "STADTRAT_ANTRAG"]
    
    job_count = 0
    
    # Iterate through all combinations
    for inst_id in institution_ids:
        for jahr in jahre_liste:
            for betragstyp in betragstypen_liste:
                
                job_payload = {
                    "institution": inst_id,
                    "jahr": jahr,
                    "betragstyp": betragstyp
                }
                
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(job_payload)
                    )
                    job_count += 1
                except Exception as e:
                    print(f"Error sending to SQS: {e}")
    
    # Small reporting in the log
    msg = f"Orchestration completed. Mode: {'DAILY' if event.get('daily') else 'FULL'}. {job_count} jobs sent."
    print(msg)
    
    return {
        'statusCode': 200,
        'body': json.dumps(msg)
    }