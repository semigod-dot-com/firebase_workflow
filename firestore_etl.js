const { Firestore } = require('@google-cloud/firestore');
const { BigQuery } = require('@google-cloud/bigquery');
const stream = require('stream');

const FS_PROJECT_ID = process.env.FS_PROJECT_ID; 
const FS_COLLECTION_PATH = process.env.FS_COLLECTION_PATH; 

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID; 
const BQ_DATASET_ID = process.env.BQ_DATASET_ID;
const BQ_STAGING_TABLE = process.env.BQ_STAGING_TABLE;
const BQ_FINAL_TABLE = process.env.BQ_FINAL_TABLE;

const TIMESTAMP_COLUMN_NAME = 'tx_time'; 
const FIRESTORE_TIMESTAMP_FIELD = 'time'; 

const FALLBACK_TIME = new Date(0);

const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
const fs = new Firestore({ projectId: FS_PROJECT_ID });

function unixToIsoString(unixTime) {
    if (typeof unixTime !== 'number' || isNaN(unixTime)) {
        throw new Error(`Invalid or missing '${FIRESTORE_TIMESTAMP_FIELD}' value for conversion.`);
    }
    
    const milliseconds = (String(unixTime).length > 10) ? unixTime : unixTime * 1000;
    
    return new Date(milliseconds).toISOString();
}

async function getLastSyncTime() {
    const tableRef = `\`${BQ_PROJECT_ID}.${BQ_DATASET_ID}.${BQ_FINAL_TABLE}\``;
    
    const query = `
        SELECT COALESCE(MAX(${TIMESTAMP_COLUMN_NAME}), CAST('${FALLBACK_TIME.toISOString()}' AS TIMESTAMP)) AS watermark
        FROM ${tableRef}
    `;

    try {
        const [rows] = await bq.query({ query: query });
        
        const watermark = rows[0].watermark ? rows[0].watermark.value : FALLBACK_TIME.toISOString();
        
        console.log(`Starting ETL with watermark: ${watermark}`);
        return new Date(watermark);

    } catch (e) {
        if (e.code === 404) {
             console.warn(`BigQuery table ${BQ_FINAL_TABLE} not found. Running full initial sync (or table needs to be created).`);
             return FALLBACK_TIME; 
        }
        console.error(`Critical error fetching watermark: ${e.message}`);
        throw e;
    }
}

async function runEtl() {
    try {
        const watermarkDate = await getLastSyncTime();
        
        const deltaSnapshot = await fs.collectionGroup(FS_COLLECTION_PATH)
            .where(FIRESTORE_TIMESTAMP_FIELD, '>=', watermarkDate.getTime() / 1000)
            .get(); 

        if (deltaSnapshot.empty) {
            console.info('No data found in Firestore since watermark. Exiting ETL.');
            return;
        }

        const deltaData = [];
        deltaSnapshot.forEach(doc => {
            const data = doc.data();
            
            const unixTimestamp = data[FIRESTORE_TIMESTAMP_FIELD]; 
            
            if (typeof unixTimestamp !== 'number') {
                console.warn(`Skipping document ${doc.id}: '${FIRESTORE_TIMESTAMP_FIELD}' is not a number.`);
                return;
            }
            
            const isoTimestamp = unixToIsoString(unixTimestamp);
            
            deltaData.push({
                document_id: doc.id,
                [TIMESTAMP_COLUMN_NAME]: isoTimestamp, 
                data_json: JSON.stringify(data)
            });
        });

        if (deltaData.length === 0) {
            console.info('No new documents found since last sync. ETL complete.');
            return;
        }

        console.log(`Extracted ${deltaData.length} new documents from Firestore.`);

        const stagingTable = bq.dataset(BQ_DATASET_ID).table(BQ_STAGING_TABLE);
        
        const jobConfig = {
            writeDisposition: 'WRITE_TRUNCATE',
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            schema: {
                fields: [
                    { name: 'document_id', type: 'STRING', mode: 'REQUIRED' },
                    { name: 'tx_time', type: 'TIMESTAMP', mode: 'REQUIRED' },
                    { name: 'data_json', type: 'STRING', mode: 'REQUIRED' }
                ]
            }
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

        console.info(`Successfully loaded ${deltaData.length} rows to BigQuery staging table ${BQ_STAGING_TABLE}.`);
        console.info('ETL Load phase finished successfully.');
        
    } catch (e) {
        console.error(`Fatal error in pipeline: ${e.message}`, e);
        process.exit(1); 
    }
}

runEtl();
