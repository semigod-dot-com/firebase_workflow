const { Firestore } = require('@google-cloud/firestore');
const { BigQuery } = require('@google-cloud/bigquery');
const stream = require('stream');

const PROJECT_ID = process.env.PROJECT_ID;
const DATASET_ID = process.env.DATASET_ID;
const STAGING_TABLE = process.env.STAGING_TABLE;
const FINAL_TABLE = process.env.FINAL_TABLE;

const FALLBACK_TIME = new Date(0); 

const bq = new BigQuery({ projectId: PROJECT_ID });
const fs = new Firestore({ projectId: PROJECT_ID });

function unixToIsoString(unixTime) {
    if (typeof unixTime !== 'number' || isNaN(unixTime)) {
        throw new Error("Invalid or missing 'updatedAt' value for conversion.");
    }
    
    const milliseconds = (String(unixTime).length > 10) ? unixTime : unixTime * 1000;
    
    return new Date(milliseconds).toISOString();
}

async function getLastSyncTime() {
    const tableRef = `\`${PROJECT_ID}.${DATASET_ID}.${FINAL_TABLE}\``;
    
    const query = `
        SELECT COALESCE(MAX(firestore_timestamp), CAST('${FALLBACK_TIME.toISOString()}' AS TIMESTAMP)) AS watermark
        FROM ${tableRef}
    `;

    try {
        const [rows] = await bq.query({ query: query });
        
        const watermark = rows[0].watermark ? rows[0].watermark.value : FALLBACK_TIME.toISOString();
        
        console.log(`Starting ETL with watermark: ${watermark}`);
        return new Date(watermark);

    } catch (e) {
        if (e.code === 404) {
             console.warn(`BigQuery table ${FINAL_TABLE} not found. Running full initial sync.`);
             return FALLBACK_TIME;
        }
        console.error(`Critical error fetching watermark: ${e.message}`);
        throw e;
    }
}

async function runEtl() {
    try {
        const watermarkDate = await getLastSyncTime();
        
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

            if (docDate > watermarkDate) {
                deltaData.push({
                    document_id: doc.id,
                    firestore_timestamp: isoTimestamp, 
                    data_json: JSON.stringify(data)
                });
            }
        });

        if (deltaData.length === 0) {
            console.info('No new documents found since last sync. ETL complete.');
            return;
        }

        console.log(`Extracted ${deltaData.length} new documents from Firestore.`);

        const stagingTable = bq.dataset(DATASET_ID).table(STAGING_TABLE);
        
        const jobConfig = {
            writeDisposition: 'WRITE_TRUNCATE',
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
        };

        const ndjsonString = deltaData.map(row => JSON.stringify(row)).join('\n');

        const dataStream = stream.Readable.from(ndjsonString);

        const loadStream = stagingTable.createWriteStream(jobConfig);

        const job = await new Promise((resolve, reject) => {
            loadStream
                .on('error', reject)
                .on('job', resolve);

            dataStream.pipe(loadStream);
        });

        const [metadata] = await job.getMetadata();

        if (metadata.status.errorResult) {
            console.error('BigQuery Load Job failed:', metadata.status.errorResult);
            throw new Error(`BigQuery Load Job failed: ${metadata.status.errorResult.message}`);
        }

        console.info(`Successfully loaded ${deltaData.length} rows to BigQuery staging table ${STAGING_TABLE} via batch job.`);
        console.info('ETL Load phase finished successfully.');
        
    } catch (e) {
        console.error(`Fatal error in pipeline: ${e.message}`, e);
        process.exit(1); 
    }
}

runEtl();
