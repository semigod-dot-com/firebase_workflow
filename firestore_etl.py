import os
from google.cloud import firestore, bigquery
from datetime import datetime, timezone

PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
STAGING_TABLE = os.environ['STAGING_TABLE']
FINAL_TABLE = os.environ['FINAL_TABLE']
FALLBACK_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)

bq_client = bigquery.Client(project=PROJECT_ID)
fs_client = firestore.Client(project=PROJECT_ID)

def get_last_sync_time():
    """Reads the current watermark from the FINAL table."""
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE}`"
    query = f"""
        SELECT COALESCE(MAX(updatedAt), '{FALLBACK_TIME.isoformat()}') 
        FROM {table_ref}
    """
    
    try:
        query_job = bq_client.query(query)
        last_sync_value = next(iter(query_job.result()))[0]
        
        if isinstance(last_sync_value, datetime):
            return last_sync_value
        else:
            return datetime.fromisoformat(last_sync_value.replace('Z', '+00:00'))
            
    except Exception as e:
        print(f"Error determining watermark: {e}")
        raise

def run_etl():
    watermark = get_last_sync_time()
    print(f"Starting ETL with watermark: {watermark.isoformat()}")

    delta_stream = fs_client.collection('products').where('updatedAt', '>', watermark).stream()
    
    delta_data = []
    for doc in delta_stream:
        data = doc.to_dict()
        delta_data.append({
            'document_id': doc.id,
            'name': data.get('name'),
            'price': data.get('price'),
            'category': data.get('category'),
            'updatedAt': data.get('updatedAt').isoformat().replace('+00:00', 'Z') if data.get('updatedAt') else None
        })
        
    if not delta_data:
        print("No new data found. Exiting ETL.")
        return

    print(f"Extracted {len(delta_data)} documents from Firestore.")

    
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") 
    
    job = bq_client.load_table_from_json(
        delta_data, 
        staging_table_id, 
        job_config=job_config
    )
    
    job.result() 
    print(f"Successfully loaded {job.output_rows} rows to {staging_table_id}.")

if __name__ == '__main__':
    run_etl()
