import boto3
import requests
import os
import re
import csv
import json
import pdfplumber

# --- CONFIGURATION ---
S3_BUCKET_NAME = "zurichcantonexpenditures"
S3_PREFIX_RAW = "raw_survey/"
S3_PREFIX_PROCESSED = "processed_survey/"

# Dictionary with URLs for the respective years
SURVEY_URLS = {
    "2019": "https://data.stadt-zuerich.ch/dataset/prd_stez_bevoelkerungsbefragungen_seit2019_od4732/download/fragebogen_bvb_2019.pdf",
    "2021": "https://data.stadt-zuerich.ch/dataset/prd_stez_bevoelkerungsbefragungen_seit2019_od4732/download/fragebogen_bvb_2021.pdf",
    "2023": "https://data.stadt-zuerich.ch/dataset/prd_stez_bevoelkerungsbefragungen_seit2019_od4732/download/fragebogen_bvb_2023.pdf"
}

s3_client = boto3.client("s3")

def extract_questions_from_pdf(pdf_path, year):
    """
    Extracts questions and adds the year.
    """
    extracted_data = []
    # Regex: Starts with S or F, followed by a number, optional letter, then a dot.
    question_pattern = re.compile(r"^(S\d+[a-z]?|F\d+[a-z]?)\.\s+(.*)")
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                match = question_pattern.match(line)
                
                if match:
                    q_id = match.group(1)
                    q_text = match.group(2)
                    
                    # Check for continuation on the next line
                    if i + 1 < len(lines):
                        next_line = lines[i+1].strip()
                        if next_line and not question_pattern.match(next_line) and not next_line.startswith("☐"):
                            q_text += " " + next_line

                    extracted_data.append({
                        "year": year,  # Add year for better assignment
                        "page": page_num + 1,
                        "id": q_id,
                        "text": q_text
                    })
                    
    return extracted_data

def lambda_handler(event, context):
    processed_files = []
    all_questions_combined = [] # Collects all questions for a master file
    errors = []

    print("Starting batch processing for years: 2019, 2021, 2023")

    for year, url in SURVEY_URLS.items():
        # Dynamically create temporary file paths
        local_pdf_path = f"/tmp/fragebogen_{year}.pdf"
        local_json_path = f"/tmp/fragen_{year}.json"
        local_csv_path = f"/tmp/fragen_{year}.csv"
        
        try:
            print(f"--- Processing year {year} ---")
            
            # 1. Download
            print(f"Downloading: {url}")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_pdf_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # 2. Upload Raw PDF
            s3_key_pdf = f"{S3_PREFIX_RAW}fragebogen_{year}.pdf"
            s3_client.upload_file(local_pdf_path, S3_BUCKET_NAME, s3_key_pdf)
            
            # 3. Extraction
            questions = extract_questions_from_pdf(local_pdf_path, year)
            all_questions_combined.extend(questions) # Add to master list
            
            # 4. Save JSON & Upload
            with open(local_json_path, 'w', encoding='utf-8') as f:
                json.dump(questions, f, ensure_ascii=False, indent=2)
            
            s3_key_json = f"{S3_PREFIX_PROCESSED}fragen_{year}.json"
            s3_client.upload_file(local_json_path, S3_BUCKET_NAME, s3_key_json)
            
            # 5. Save CSV & Upload
            with open(local_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Year", "Page", "ID", "Question"])
                for q in questions:
                    writer.writerow([q["year"], q["page"], q["id"], q["text"]])
            
            s3_key_csv = f"{S3_PREFIX_PROCESSED}fragen_{year}.csv"
            s3_client.upload_file(local_csv_path, S3_BUCKET_NAME, s3_key_csv)
            
            processed_files.append(f"Year {year}: OK ({len(questions)} questions)")
            
            # Cleanup
            if os.path.exists(local_pdf_path): os.remove(local_pdf_path)

        except Exception as e:
            error_msg = f"Error with year {year}: {str(e)}"
            print(error_msg)
            errors.append(error_msg)
            # We continue with the next year even if one fails

    # 6. Create a master CSV with all years
    if all_questions_combined:
        try:
            master_csv_path = "/tmp/fragen_ALLE_JAHRE.csv"
            with open(master_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Year", "Page", "ID", "Question"])
                for q in all_questions_combined:
                    writer.writerow([q["year"], q["page"], q["id"], q["text"]])
            
            s3_key_master = f"{S3_PREFIX_PROCESSED}fragen_COMBINED_2019_2023.csv"
            s3_client.upload_file(master_csv_path, S3_BUCKET_NAME, s3_key_master)
            processed_files.append(f"MASTER CSV created: {s3_key_master}")
        except Exception as e:
            print(f"Could not create master CSV: {e}")

    return {
        'statusCode': 200 if not errors else 206, # 206 = Partial Content
        'body': json.dumps({
            'summary': processed_files,
            'errors': errors
        })
    }