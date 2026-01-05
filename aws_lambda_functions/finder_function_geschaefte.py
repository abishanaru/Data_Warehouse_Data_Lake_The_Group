import boto3
import requests
import os
import json
import urllib3
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta  # timedelta added for date calculation

# --- Configuration ---
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL') 

# IMPORTANT: Use HTTPS
SEARCH_URL = "https://www.gemeinderat-zuerich.ch/api/geschaeft/searchdetails"

# Disable SSL warnings
SSL_VERIFY = False
if not SSL_VERIFY:
    urllib3.disable_warnings()

sqs_client = boto3.client("sqs")

NAMESPACES = {
    'resp': 'http://www.cmiag.ch/cdws/searchDetailResponse',
    'ges': 'http://www.cmiag.ch/cdws/Geschaeft'
}

def lambda_handler(event, context):
    print("Starting 'Finder' function...")
    
    total_messages_sent = 0
    search_intervals = []

    # --- 1. Decision: Daily Mode or Year Range? ---
    if event.get('daily'):
        # Daily Mode: Fetch everything from yesterday
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        year_val = yesterday.year
        
        print(f"Mode: DAILY. Searching data for: {yesterday_str}")
        
        search_intervals.append({
            'label': f"Day {yesterday_str}",
            'start_time': f"{yesterday_str} 00:00:00",
            'end_time': f"{yesterday_str} 23:59:59",
            'year_source': year_val
        })
        
    else:
        # Standard Mode: Search across years
        start_year = int(event.get("start_year", 2024))
        end_year = int(event.get("end_year", 2025))
        print(f"Mode: RANGE. Searching years {start_year} to {end_year}")
        
        for y in range(start_year, end_year + 1):
            search_intervals.append({
                'label': f"Year {y}",
                'start_time': f"{y}-01-01 00:00:00",
                'end_time': f"{y}-12-31 23:59:59",
                'year_source': y
            })

    # --- 2. Processing defined intervals ---
    for interval in search_intervals:
        
        # Safety check: If less than 30 seconds remain
        if context.get_remaining_time_in_millis() < 30000:
            print(f"WARNING: Lambda time almost up. Stopping before interval {interval['label']}.")
            break

        print(f"--- Starting processing for {interval['label']} ---")
        
        # Dynamically build query with times from the interval
        query_string = (
            f'beginn_start > "{interval["start_time"]}" AND '
            f'beginn_start < "{interval["end_time"]}" '
            f'sortBy beginn_start/sort.ascending'
        )

        search_params = {
            'q': query_string,
            'l': 'de-CH',
            'm': '100' # Batch size
        }

        current_s = 1
        interval_hits_processed = 0
        total_hits_interval = None
        
        while True:
            # Check timeout within pagination as well
            if context.get_remaining_time_in_millis() < 10000:
                print("CRITICAL: Time almost up. Aborting pagination.")
                break

            pag_params = search_params.copy()
            pag_params['s'] = str(current_s)
            
            try:
                r_search = requests.get(SEARCH_URL, params=pag_params, verify=SSL_VERIFY, timeout=30)
                r_search.raise_for_status()
                
                root = ET.fromstring(r_search.content)
                
                if total_hits_interval is None:
                    total_hits_interval = int(root.attrib.get('numHits', 0))
                    print(f"{interval['label']}: {total_hits_interval} hits found.")
                    if total_hits_interval == 0:
                        break

                hits = root.findall('.//resp:Hit', NAMESPACES)
                if not hits:
                    break

                # --- PDF Processing Logic ---
                for hit in hits:
                    geschaeft = hit.find('ges:Geschaeft', NAMESPACES)
                    if geschaeft is None: continue

                    grnr_elem = geschaeft.find('ges:GRNr', NAMESPACES)
                    titel_elem = geschaeft.find('ges:Titel', NAMESPACES)
                    
                    grnr = grnr_elem.text if grnr_elem is not None else None
                    titel = titel_elem.text if titel_elem is not None else "No Title"
                    
                    if not grnr: continue

                    pdf_files_list = []
                    ablaufschritte = geschaeft.find('ges:Ablaufschritte', NAMESPACES)
                    
                    if ablaufschritte is not None:
                        for aufgabe in ablaufschritte.findall('ges:Aufgabe', NAMESPACES):
                            dokumente_container = aufgabe.find('ges:Dokumente', NAMESPACES)
                            if dokumente_container is None: continue
                                
                            for dok in dokumente_container.findall('ges:Dokument', NAMESPACES):
                                dok_titel_elem = dok.find('ges:Titel', NAMESPACES)
                                if dok_titel_elem is not None and dok_titel_elem.text:
                                    dok_titel = dok_titel_elem.text
                                    
                                    # Filter: Only final votes (Schlussabstimmung)
                                    if 'schlussabstimmung' in dok_titel.lower():
                                        file_elem = dok.find('ges:File', NAMESPACES)
                                        if file_elem is not None:
                                            file_id = file_elem.attrib.get('ID')
                                            if file_id:
                                                pdf_files_list.append({
                                                    'titel': dok_titel, 
                                                    'file_id': file_id
                                                })

                    if pdf_files_list:
                        # Send to SQS
                        sqs_client.send_message(
                            QueueUrl=SQS_QUEUE_URL,
                            MessageBody=json.dumps({
                                'grnr': grnr,
                                'titel': titel,
                                'pdf_files': pdf_files_list,
                                'year_source': interval['year_source'] # Use year from interval object
                            })
                        )
                        total_messages_sent += 1

                # Continue pagination
                current_s += len(hits)
                interval_hits_processed += len(hits)
                
                print(f"{interval['label']}: {interval_hits_processed}/{total_hits_interval} processed.")

                if current_s > total_hits_interval:
                    break
                    
            except Exception as e:
                print(f"ERROR in {interval['label']} at index {current_s}: {e}")
                break
    
    return {
        'statusCode': 200,
        'body': f'Done. {total_messages_sent} messages sent to SQS.'
    }