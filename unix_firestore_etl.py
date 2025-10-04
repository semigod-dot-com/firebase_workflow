import os
import json
import logging
from google.cloud import firestore, bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timezone
from dateutil.parser import parse

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration (Read from Environment) ---
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID')
STAGING_TABLE = os.environ.get('STAGING_TABLE')
FINAL_TABLE = os.environ.get('FINAL_TABLE')
FALLBACK_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
# ---------------------------------------------

# Initialize clients
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
    query = f"""
        SELECT COALESCE(MAX(firestore_timestamp), CAST('{FALLBACK_TIME.isoformat()}' AS TIMESTAMP)) AS watermark
        FROM {table_ref}
    """
    
    try:
        query_job = bq_client.query(query)
        # Fetch the single result row
        last_sync_value = next(iter(query_job.result()))[0]
        
        if isinstance(last_sync_value, datetime):
            return last_sync_value.replace(tzinfo=timezone.utc)
        # Handle the fallback ISO string case
        elif isinstance(last_sync_value, str):
            return parse(last_sync_value).replace(tzinfo=timezone.utc)
        else:
            return FALLBACK_TIME
            
    except NotFound:
        logging.warning(f"BigQuery table {table_ref} not found. Running full initial sync.")
        return FALLBACK_TIME
    except Exception as e:
        logging.error(f"Critical error fetching watermark: {e}")
        raise


def unix_to_iso(unix_time: int) -> str:
    """Converts Unix epoch (milliseconds) to ISO 8601 string (required by BQ)."""
    # Assuming milliseconds (length > 10). If seconds, remove '/ 1000'.
    if len(str(unix_time)) > 10:
        # Convert milliseconds to seconds for fromtimestamp
        dt_object = datetime.fromtimestamp(unix_time / 1000, tz=timezone.utc)
    else:
        # Assume seconds
        dt_object = datetime.fromtimestamp(unix_time, tz=timezone.utc)

    # Convert to ISO 8601 string (e.g., 2025-10-04T08:00:00+00:00)
    return dt_object.isoformat().replace('+00:00', 'Z')


def run_etl():
    """Executes the Extract and Load (E/L) phase of the pipeline."""
    try:
        # 1. Determine Watermark
        # Since we compare Firestore 'updatedAt' (Unix) to BQ 'firestore_timestamp' (TIMESTAMP),
        # we need to convert the BQ timestamp to Unix time for the query.
        watermark_dt = get_last_sync_time()
        
        # Convert BQ datetime watermark to Unix milliseconds for Firestore filtering
        # The query will now be executed client-side after retrieving ALL documents.
        # This is less efficient than a server-side query, but necessary if Firestore stores Unix time as a number.
        logging.info(f"Starting incremental sync with watermark: {watermark_dt.isoformat()}")

        # 2. Extract Delta from Firestore (NO server-side filtering possible with Unix number)
        # We must retrieve all and filter client-side, OR use the less efficient method below:
        
        # --- Using client-side filtering (Safer if Firestore saves the Unix time as an INT/FLOAT field) ---
        delta_stream = fs_client.collection('products').stream()
        
        delta_data = []
        for doc in delta_stream:
            data = doc.to_dict()
            unix_timestamp = data.get('updatedAt') # This is the Unix epoch integer
            
            if not unix_timestamp:
                logging.warning(f"Skipping document {doc.id}: Missing 'updatedAt'.")
                continue

            # Check if this record is newer than the last sync time
            iso_timestamp = unix_to_iso(unix_timestamp)
            if parse(iso_timestamp).replace(tzinfo=timezone.utc) <= watermark_dt:
                 continue # Skip old records
            
            # 3. Construct the row for the STAGING table
            delta_data.append({
                'document_id': doc.id,
                'firestore_timestamp': iso_timestamp,
                'data_json': json.dumps(data)
            })
            
        if not delta_data:
            logging.info("No new documents found since last sync. ETL complete.")
            return

        logging.info(f"Successfully extracted {len(delta_data)} new documents.")

        # 4. Load to BigQuery Staging (TRUNCATE then Load)
        staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") 
        
        job = bq_client.load_table_from_json(
            delta_data, 
            staging_table_id, 
            job_config=job_config
        )
        
        job.result()
        logging.info(f"Successfully loaded {job.output_rows} rows to {staging_table_id}.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        exit(1) 


if __name__ == '__main__':
    run_etl()
