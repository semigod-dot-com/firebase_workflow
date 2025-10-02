import os
import json
import logging
from google.cloud import firestore, bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timezone

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration (Read from Environment) ---
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID')
STAGING_TABLE = os.environ.get('STAGING_TABLE')
FINAL_TABLE = os.environ.get('FINAL_TABLE')
FALLBACK_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
# ---------------------------------------------

# Initialize clients (Authentication handled automatically by GitHub Action's credentials)
try:
    if not all([PROJECT_ID, DATASET_ID, STAGING_TABLE, FINAL_TABLE]):
        raise EnvironmentError("Missing required environment variables. Check your YAML file.")
        
    bq_client = bigquery.Client(project=PROJECT_ID)
    fs_client = firestore.Client(project=PROJECT_ID)
    logging.info("BigQuery and Firestore clients initialized successfully.")
    
except Exception as e:
    logging.error(f"Initialization failed: {e}")
    exit(1)


def get_last_sync_time() -> datetime:
    """Queries BigQuery for the latest firestore_timestamp in the FINAL table."""
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE}`"
    # COALESCE ensures we always get a valid timestamp (or the fallback time)
    query = f"""
        SELECT COALESCE(MAX(updatedAt), '{FALLBACK_TIME.isoformat()}') 
        FROM {table_ref}
    """
    
    try:
        query_job = bq_client.query(query)
        # Fetch the single result row
        last_sync_value = next(iter(query_job.result()))[0]
        
        if isinstance(last_sync_value, datetime):
            return last_sync_value.replace(tzinfo=timezone.utc)
        else:
            # Fallback returns an ISO string, parse it into a datetime object
            return datetime.fromisoformat(last_sync_value.replace('Z', '+00:00'))
            
    except NotFound:
        logging.warning(f"BigQuery table {table_ref} not found. Running full initial sync.")
        return FALLBACK_TIME
    except Exception as e:
        logging.error(f"Critical error fetching watermark: {e}")
        raise # Re-raise to fail the pipeline


def run_etl():
    """Executes the Extract and Load (E/L) phase of the pipeline."""
    try:
        # 1. Determine Watermark
        watermark = get_last_sync_time()
        logging.info(f"Starting incremental sync with watermark: {watermark.isoformat()}")

        # 2. Extract Delta from Firestore
        # The query uses the timestamp watermark to fetch only new/updated documents
        delta_stream = (
            fs_client.collection('products')
            .where('updatedAt', '>', watermark)
            .order_by('updatedAt', direction=firestore.Query.ASCENDING)
            .stream()
        )
        
        delta_data = []
        for doc in delta_stream:
            data = doc.to_dict()
            
            # Use the 'updatedAt' field for the top-level BigQuery timestamp
            watermark_time = data.pop('updatedAt', None)
            
            if not watermark_time:
                logging.warning(f"Skipping document {doc.id}: Missing 'updatedAt' field.")
                continue

            # Construct the row for the STAGING table
            delta_data.append({
                'document_id': doc.id,
                # Convert Timestamp to ISO string with Z for BQ ingestion compatibility
                'firestore_timestamp': watermark_time.isoformat().replace('+00:00', 'Z'),
                'data_json': json.dumps(data) # Stringify the rest of the document data
            })
            
        if not delta_data:
            logging.info("No new documents found since last sync. ETL complete.")
            return

        logging.info(f"Successfully extracted {len(delta_data)} documents from Firestore.")

        # 3. Load to BigQuery Staging (TRUNCATE then Load)
        staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"
        
        # WRITE_TRUNCATE clears the table before inserting the delta_data
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") 
        
        job = bq_client.load_table_from_json(
            delta_data, 
            staging_table_id, 
            job_config=job_config
        )
        
        job.result() # Wait for the job to complete
        logging.info(f"Successfully loaded {job.output_rows} rows to {staging_table_id}.")
        logging.info("ETL Load phase finished successfully. Awaiting BigQuery MERGE job.")

    except Exception as e:
        logging.error(f"Pipeline failed during ETL: {e}", exc_info=True)
        # Exit with a non-zero code to fail the GitHub Action
        exit(1) 


if __name__ == '__main__':
    run_etl()
