import boto3
import os
import re
import csv
import json
import pdfplumber

# --- CONFIGURATION ---
S3_BUCKET_NAME = "zurichcantonexpenditures"
S3_PREFIX_INPUT = "raw_paris_api/pdf/"
S3_PREFIX_OUTPUT = "processed_paris_api/"

s3_client = boto3.client("s3")

def extract_metadata_from_pdf(pdf_path, filename):
    """
    Opens the PDF and extracts metadata from the first page.
    """
    data = {
        "Dateiname": filename,
        "Geschäftstitel": None,
        "Geschäftsnummer": None,
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
            # Metadata is always on the first page
            first_page_text = pdf.pages[0].extract_text()
            
            if not first_page_text:
                return data

            # --- REGEX EXTRACTION ---
            # 1. Business title (can span multiple lines, hence re.DOTALL)
            # Searches text between "Geschäftstitel:" and "Geschäfts#:"
            title_match = re.search(r"Geschäftstitel:(.*?)Geschäfts#:", first_page_text, re.DOTALL)
            if title_match:
                # Replace line breaks in title with spaces and clean up
                data["Geschäftstitel"] = title_match.group(1).replace('\n', ' ').strip()

            # 2. Business number
            num_match = re.search(r"Geschäfts#:\s*([\d/]+)", first_page_text)
            if num_match: data["Geschäftsnummer"] = num_match.group(1)

            # 3. Date (Format DD.MM.YYYY)
            date_match = re.search(r"Stimm-Datum:\s*(\d{2}\.\d{2}\.\d{4})", first_page_text)
            if date_match: data["Stimm_Datum"] = date_match.group(1)

            # 4. Time
            time_match = re.search(r"Stimm-Zeit:\s*(\d{2}:\d{2}:\d{2})", first_page_text)
            if time_match: data["Stimm_Zeit"] = time_match.group(1)

            # 5. Voting numbers (YES, NO, Abstentions, etc.)
            # We look for the label followed by a number
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

            # 6. Tie-breaker (Stichentscheid)
            # Takes the text after "Stichentscheid:", often empty or "Stadt Zürich"
            stich_match = re.search(r"Stichentscheid:\s*(.*)", first_page_text)
            if stich_match:
                clean_stich = stich_match.group(1).strip()
                # If "Stadt Zürich" comes directly after, filter it out
                if "Stadt Zürich" not in clean_stich:
                    data["Stichentscheid"] = clean_stich
                else:
                    data["Stichentscheid"] = "" # Leave empty if no text is present

    except Exception as e:
        print(f"Error parsing {filename}: {e}")
    
    return data

def lambda_handler(event, context):
    all_results = []
    processed_count = 0
    error_count = 0
    
    # Use paginator if there are more than 1000 PDFs in the folder
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX_INPUT)

    print("Starting processing...")

    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Process only PDFs, skip folders
            if not key.lower().endswith('.pdf'):
                continue

            filename = os.path.basename(key)
            local_path = f"/tmp/{filename}"

            try:
                # Download
                s3_client.download_file(S3_BUCKET_NAME, key, local_path)
                
                # Extract
                metadata = extract_metadata_from_pdf(local_path, filename)
                all_results.append(metadata)
                processed_count += 1
                
                # Cleanup to save storage space in /tmp
                os.remove(local_path)
                
                # Small log every 10 files
                if processed_count % 10 == 0:
                    print(f"{processed_count} files processed...")

            except Exception as e:
                print(f"Error with file {key}: {e}")
                error_count += 1

    # --- SAVE ---
    
    if all_results:
        # 1. Save as JSON
        json_key = f"{S3_PREFIX_OUTPUT}abstimmungen_alle.json"
        local_json = "/tmp/output.json"
        with open(local_json, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        s3_client.upload_file(local_json, S3_BUCKET_NAME, json_key)

        # 2. Save as CSV
        csv_key = f"{S3_PREFIX_OUTPUT}abstimmungen_alle.csv"
        local_csv = "/tmp/output.csv"
        
        # Take header from the keys of the first result
        fieldnames = list(all_results[0].keys())
        
        with open(local_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        s3_client.upload_file(local_csv, S3_BUCKET_NAME, csv_key)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Done',
            'processed': processed_count,
            'errors': error_count,
            'output_json': json_key if all_results else None,
            'output_csv': csv_key if all_results else None
        })
    }