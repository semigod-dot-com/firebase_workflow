// firestore_etl.js

const { Firestore } = require('@google-cloud/firestore');
const { BigQuery } = require('@google-cloud/bigquery');
const stream = require('stream'); // REQUIRED for batch loading

// --- Configuration ---
// Environment variables MUST be set in your GitHub Actions workflow
const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const STAGING_TABLE = process.env.STAGING_TABLE;
const FINAL_TABLE = process.env.FINAL_TABLE;

// Fallback time for the first run (1970-01-01T00:00:00.000Z)
const FALLBACK_TIME = new Date(0); 

// Initialize clients (assumes service account credentials are set by GitHub Actions)
const bq = new BigQuery({ projectId: PROJECT_ID });
const fs = new Firestore({ projectId: PROJECT_ID });

// --- Helper Functions ---

/**
 * Converts a Unix epoch timestamp (assumes milliseconds) to an ISO 8601 string.
 * @param {number} unixTime - The Unix timestamp integer.
 * @returns {string} ISO 8601 string (e.g., "2025-10-04T13:54:03.000Z").
 */
function unixToIsoString(unixTime) {
    if (typeof unixTime !== 'number' || isNaN(unixTime)) {
        throw new Error("Invalid or missing 'updatedAt' value for conversion.");
    }
    
    // Check for millisecond precision (typical if length > 10 digits)
    const milliseconds = (String(unixTime).length > 10) ? unixTime : unixTime * 1000;
    
    // Date constructor handles milliseconds since epoch. toISOString() is required by BQ.
    return new Date(milliseconds).toISOString();
}

/**
 * Queries BigQuery for the latest firestore_timestamp in the FINAL table.
 * @returns {Promise<Date>} The latest synchronization time.
 */
async function getLastSyncTime() {
    const tableRef = `\`${PROJECT_ID}.${DATASET_ID}.${FINAL_TABLE}\``;
    
    // NOTE: Querying the correct column: firestore_timestamp
    const query = `
        SELECT COALESCE(MAX(firestore_timestamp), CAST('${FALLBACK_TIME.toISOString()}' AS TIMESTAMP)) AS watermark
        FROM ${tableRef}
    `;

    try {
        // FIX: Correctly destructure [rows] from the bq.query method
        const [rows] = await bq.query({ query: query });
        
        // rows[0].watermark will be a BigQuery TIMESTAMP object
        const watermark = rows[0].watermark ? rows[0].watermark.value : FALLBACK_TIME.toISOString();
        
        console.log(`Starting ETL with watermark: ${watermark}`);
        return new Date(watermark);

    } catch (e) {
        // Handle table not found error on initial run
        if (e.code === 404) {
             console.warn(`BigQuery table ${FINAL_TABLE} not found. Running full initial sync.`);
             return FALLBACK_TIME;
        }
        console.error(`Critical error fetching watermark: ${e.message}`);
        throw e;
    }
}

// --- Main ETL Logic ---

async function runEtl() {
    try {
        // 1. Determine Watermark
        const watermarkDate = await getLastSyncTime();
        
        // 2. Extract Delta from Firestore
        const deltaSnapshot = await fs.collection('products').get(); 

        if (deltaSnapshot.empty) {
            console.info('No data found in Firestore. Exiting ETL.');
            return;
        }

        const deltaData = [];
        deltaSnapshot.forEach(doc => {
            const data = doc.data();
            const unixTimestamp = data.updatedAt; 
            
            if (typeof unixTimestamp !== 'number') {
                console.warn(`Skipping document ${doc.id}: 'updatedAt' is not a number.`);
                return;
            }
            
            const isoTimestamp = unixToIsoString(unixTimestamp);
            const docDate = new Date(isoTimestamp);

            // Client-side filtering check (Only keep records STRICTLY newer than the watermark)
            if (docDate > watermarkDate) {
                // 3. Construct the row
