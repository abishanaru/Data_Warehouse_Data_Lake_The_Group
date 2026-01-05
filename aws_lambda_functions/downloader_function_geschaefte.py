import boto3
import requests
import os
import json
import urllib3
import re
import pdfplumber
from botocore.exceptions import ClientError

# --- Configuration ---
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
RAW_JSON_PREFIX = os.environ.get('RAW_JSON_PREFIX', 'raw_paris_api/json/')
RAW_PDF_PREFIX = os.environ.get('RAW_PDF_PREFIX', 'raw_paris_api/pdf/')
PROCESSED_PREFIX = os.environ.get('PROCESSED_PREFIX', 'processed_paris_api/')

PDF_DOWNLOAD_BASE_URL = "https://www.gemeinderat-zuerich.ch/dokumente/"

# Disable SSL warnings
SSL_VERIFY = False
if not SSL_VERIFY:
    urllib3.disable_warnings()

s3_client = boto3.client("s3")

def check_if_exists(bucket, key):
    """Efficiently checks if a file already exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def extract_metadata_from_pdf(pdf_path, filename, grnr_context=None):
    """
    Extracts data from the PDF.
    grnr_context: The business number from the SQS Message (as fallback/supplement)
    """
    data = {
        "Dateiname": filename,
        "Geschäftsnummer_Input": grnr_context, # Value from SQS
        "Geschäftstitel": None,
        "Geschäftsnummer_PDF": None, # Value from PDF
        "Stimm_Datum": None,
        "Stimm_Zeit": None,
        "JA": None,
        "NEIN": None,
        "Enthalten": None,
        "Nicht_Praesent": None,
        "Total_Stimmen": None,
        "Stichentscheid": None
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages: return data
            first_page_text = pdf.pages[0].extract_text()
            
            if not first_page_text:
                return data

            # --- REGEX LOGIC (As in your template) ---
            
            # 1. Business title
            title_match = re.search(r"Geschäftstitel:(.*?)Geschäfts#:", first_page_text, re.DOTALL)
            if title_match:
                data["Geschäftstitel"] = title_match.group(1).replace('\n', ' ').strip()

            # 2. Business number
            num_match = re.search(r"Geschäfts#:\s*([\d/]+)", first_page_text)
            if num_match: data["Geschäftsnummer_PDF"] = num_match.group(1)

            # 3. Date
            date_match = re.search(r"Stimm-Datum:\s*(\d{2}\.\d{2}\.\d{4})", first_page_text)
            if date_match: data["Stimm_Datum"] = date_match.group(1)

            # 4. Time
            time_match = re.search(r"Stimm-Zeit:\s*(\d{2}:\d{2}:\d{2})", first_page_text)
            if time_match: data["Stimm_Zeit"] = time_match.group(1)

            # 5. Numbers
            ja_match = re.search(r"JA:\s*(\d+)", first_page_text)
            if ja_match: data["JA"] = int(ja_match.group(1))

            nein_match = re.search(r"NEIN:\s*(\d+)", first_page_text)
            if nein_match: data["NEIN"] = int(nein_match.group(1))

            ent_match = re.search(r"Enthalten:\s*(\d+)", first_page_text)
            if ent_match: data["Enthalten"] = int(ent_match.group(1))

            np_match = re.search(r"Nicht Präsent:\s*(\d+)", first_page_text)
            if np_match: data["Nicht_Praesent"] = int(np_match.group(1))

            total_match = re.search(r"Total Stimmen:\s*(\d+)", first_page_text)
            if total_match: data["Total_Stimmen"] = int(total_match.group(1))

            # 6. Tie-breaker vote (Stichentscheid)
            stich_match = re.search(r"Stichentscheid:\s*(.*)", first_page_text)
            if stich_match:
                clean_stich = stich_match.group(1).strip()
                if "Stadt Zürich" not in clean_stich:
                    data["Stichentscheid"] = clean_stich
                else:
                    data["Stichentscheid"] = ""

    except Exception as e:
        print(f"Error parsing (PDFPlumber) {filename}: {e}")
        # We return the empty/partial data, do not raise an error, 
        # so the process continues
    
    return data

def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        message_body = json.loads(record['body'])
        
        grnr = message_body.get('grnr')
        titel = message_body.get('titel')
        pdf_files = message_body.get('pdf_files', [])
        
        if not grnr:
            print("Error: No 'grnr' in message.")
            return {'statusCode': 200, 'body': 'Invalid message'}
            
    except Exception as e:
        print(f"Error reading SQS: {e}")
        raise e 

    print(f"Starting processing for GRNr: {grnr} with {len(pdf_files)} PDFs")
    
    s3_filename_base = grnr.replace('/', '-')

    try:
        # 1. Save metadata JSON (Raw from SQS)
        json_s3_key = f"{RAW_JSON_PREFIX}{s3_filename_base}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=json_s3_key,
            Body=json.dumps(message_body, indent=2),
            ContentType='application/json'
        )

        if not pdf_files:
            return {'statusCode': 200, 'body': 'No PDFs to process.'}

        stats = {'downloaded': 0, 'parsed': 0, 'skipped': 0}
        
        for i, pdf_file in enumerate(pdf_files):
            file_id = pdf_file.get('file_id')
            pdf_titel = pdf_file.get('titel', 'unknown')
            
            if not file_id: continue
            
            # Define filenames
            pdf_filename = f"{s3_filename_base}_abstimmung_{i+1}.pdf"
            pdf_s3_key = f"{RAW_PDF_PREFIX}{pdf_filename}"
            processed_json_key = f"{PROCESSED_PREFIX}{s3_filename_base}_abstimmung_{i+1}.json"
            
            # Temporary path
            local_path = f"/tmp/{pdf_filename}"

            # If PDF is already there AND the processed JSON is already there, we skip everything.
            if check_if_exists(S3_BUCKET_NAME, pdf_s3_key) and check_if_exists(S3_BUCKET_NAME, processed_json_key):
                print(f"Already completely processed: {pdf_filename}")
                stats['skipped'] += 1
                continue

            # --- A) Download ---
            pdf_url = f"{PDF_DOWNLOAD_BASE_URL}{file_id}"
            
            try:
                print(f"Downloading: {pdf_url}")
                r_pdf = requests.get(pdf_url, verify=SSL_VERIFY, timeout=30)
                r_pdf.raise_for_status()
                
                # Save locally for processing
                with open(local_path, 'wb') as f:
                    f.write(r_pdf.content)
                
                # Upload to RAW directory (Backup/Archive)
                s3_client.upload_file(local_path, S3_BUCKET_NAME, pdf_s3_key)
                stats['downloaded'] += 1
                
                # --- B) Parsing (Immediately following) ---
                print(f"Parsing PDF: {local_path}")
                extracted_data = extract_metadata_from_pdf(local_path, pdf_filename, grnr_context=grnr)
                
                # Save parsing result
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=processed_json_key,
                    Body=json.dumps(extracted_data, ensure_ascii=False, indent=2),
                    ContentType='application/json'
                )
                stats['parsed'] += 1
                print(f"Result saved: {processed_json_key}")

            except Exception as e_proc:
                print(f"Error with file {pdf_filename}: {e_proc}")
                # We continue with the next PDF in the list
            
            finally:
                # Cleanup in /tmp
                if os.path.exists(local_path):
                    os.remove(local_path)

        return {
            'statusCode': 200,
            'body': json.dumps(stats)
        }
        
    except Exception as e_inner:
        print(f"CRITICAL ERROR: {e_inner}")
        raise e_inner