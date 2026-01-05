import boto3
import requests
import json
import xmltodict
from datetime import datetime, timedelta

# Configuration
S3_BUCKET_NAME = "zurichcantonexpenditures"
S3_KEY_PREFIX = "raw_paris_api/geschaefte"
BASE_URL = "http://www.gemeinderat-zuerich.ch/api/geschaeft/searchdetails"
PAGE_SIZE = 1000 

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    try:
        # 1. DETERMINE TIME RANGE
        is_initial_load = event.get('test') == 'initial'
        now = datetime.now()
        
        if is_initial_load:
            print("Mode: Initial Load")
            start_date_str = "2019-01-01 00:00:00"
            end_date_str = now.strftime("%Y-%m-%d %H:%M:%S")
        else:
            print("Mode: Daily Update")
            today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_midnight = today_midnight - timedelta(days=1)
            start_date_str = yesterday_midnight.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = today_midnight.strftime("%Y-%m-%d %H:%M:%S")

        query_string = f'beginn_start > "{start_date_str}" AND beginn_start < "{end_date_str}" sortBy beginn_start/sort.ascending'
        query_params = {'q': query_string, 'l': 'de-CH', 'm': str(PAGE_SIZE)}

        current_s = 1
        page_count = 0

        while True:
            page_count += 1
            print(f"Loading page {page_count} (starting at hit {current_s})...")
            
            loop_params = query_params.copy()
            loop_params['s'] = str(current_s)

            r = requests.get(BASE_URL, params=loop_params)
            r.raise_for_status()
            
            # 1. Parse & Clean
            # force_list: We also force 'Departement' into a list, 
            # in case there are multiple involved departments.
            data_dict = xmltodict.parse(
                r.content,
                process_namespaces=True,
                namespaces={
                    'http://www.cmiag.ch/cdws/searchDetailResponse': None,
                    'http://www.cmiag.ch/cdws/Geschaeft': None
                },
                # We add 'Departement' so Glue doesn't get confused later 
                # if it's sometimes an array and sometimes an object.
                force_list={'Hit', 'Dokument', 'Departement'} 
            )

            # 2. Find root
            root_key = list(data_dict.keys())[0]
            root = data_dict[root_key]

            # 3. Normalize hits
            if 'Hit' not in root or root['Hit'] is None:
                root['Hit'] = []
            
            hits = root['Hit']
            for hit in hits:
                if 'Geschaeft' in hit:
                    biz = hit['Geschaeft']
                    
                    # A. Repair documents
                    if 'Dokument' not in biz or biz['Dokument'] is None:
                        biz['Dokument'] = []
                        
                    # B. Ensure lead department exists
                    # (So Glue doesn't crash if it's missing)
                    if 'FederfuehrendesDepartement' not in biz:
                        biz['FederfuehrendesDepartement'] = {}
                        
                    # C. Ensure involved departments exist
                    if 'MitbeteiligteDepartemente' not in biz:
                        biz['MitbeteiligteDepartemente'] = {}

            # 4. Get metadata
            if '@numHits' in root:
                total_hits = int(root['@numHits'])
            elif 'numHits' in root: 
                total_hits = int(root['numHits'])
            else:
                total_hits = 0

            if page_count == 1:
                print(f"API reports {total_hits} total hits.")
                if total_hits == 0:
                    break

            # --- S3 Upload ---
            if is_initial_load:
                filename = f"initial_page_{page_count}.json"
            else:
                date_part = start_date_str.split(" ")[0]
                filename = f"daily_{date_part}_page_{page_count}.json"

            s3_key = f"{S3_KEY_PREFIX}/{filename}"
            
            s3_client.put_object(
                Body=json.dumps(data_dict, ensure_ascii=False, indent=2).encode('utf-8'), 
                Bucket=S3_BUCKET_NAME, 
                Key=s3_key,
                ContentType='application/json'
            )
            print(f"Saved: {s3_key}")

            if current_s + PAGE_SIZE > total_hits:
                print("All pages processed.")
                break
            
            current_s += PAGE_SIZE

    except Exception as e:
        print(f"ERROR: {e}")
        raise e

    return {'statusCode': 200, 'body': 'Done'}