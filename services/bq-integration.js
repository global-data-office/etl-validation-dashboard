// services/bq-integration.js - COMPLETE FIXED: Preserve original field names
const { BigQuery } = require('@google-cloud/bigquery');

class BigQueryIntegrationService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        
        this.config = {
            tempDataset: 'temp_validation_tables',
            tempTablePrefix: 'json_temp_',
            maxBatchSize: 1000
        };
        
        console.log('BigQuery Integration Service initialized - PRESERVE CASE VERSION');
        console.log(`Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
        console.log('FIXED: Now preserves original field names for proper matching');
    }

    /**
     * FIXED: Clean field name while preserving case for matching
     */
    cleanFieldName(fieldName) {
        // Only clean invalid characters, DON'T change case
        return fieldName
            .replace(/[^a-zA-Z0-9_]/g, '_')  // Replace invalid chars with underscore
            .replace(/^[0-9]/, '_$&')        // Prefix numbers with underscore
            .substring(0, 128);              // Limit length - NO .toLowerCase()!
    }

    /**
     * Clean data value for BigQuery compatibility
     */
    cleanDataValue(value, fieldName) {
        if (value === null || value === undefined) {
            return null;
        }

        let cleanValue = String(value).trim();
        
        if (cleanValue.length > 50000) {
            console.warn(`Truncating long value for field ${fieldName}`);
            cleanValue = cleanValue.substring(0, 50000) + '... [TRUNCATED]';
        }

        cleanValue = cleanValue
            .replace(/\0/g, '')
            .replace(/\r\n/g, '\n')
            .replace(/\r/g, '\n')
            .replace(/[\x00-\x1F\x7F]/g, ' ');

        return cleanValue;
    }

    async testConnection() {
        try {
            console.log('Testing BigQuery connection...');
            const query = `SELECT 1 as test_connection`;
            const [rows] = await this.bigquery.query(query);
            console.log('BigQuery connection successful');
            return { success: true, message: 'Connection established' };
        } catch (error) {
            console.error('BigQuery connection failed:', error.message);
            throw error;
        }
    }

    /**
     * Test access to any source table
     */
    async testSourceTableAccess(sourceTable) {
        try {
            console.log('Testing access to source table...');
            console.log(`Table: ${sourceTable}`);
            
            const countQuery = `SELECT COUNT(*) as record_count FROM \`${sourceTable}\` LIMIT 1`;
            const [countResults] = await this.bigquery.query(countQuery);
            const recordCount = countResults[0].record_count;
            
            console.log(`Source table access successful`);
            console.log(`Record count: ${recordCount}`);
            
            return { 
                success: true, 
                message: 'Source table accessible',
                recordCount: parseInt(recordCount),
                tableName: sourceTable
            };
            
        } catch (error) {
            console.error('Source table access failed:', error.message);
            
            if (error.message.includes('not found')) {
                throw new Error(`Table not found: ${sourceTable}. Please verify the table exists.`);
            } else if (error.message.includes('permission')) {
                throw new Error(`Permission denied accessing: ${sourceTable}. Please check your BigQuery permissions.`);
            } else {
                throw new Error(`Failed to access source table: ${error.message}`);
            }
        }
    }

    async initializeTempDataset() {
        try {
            console.log(`Initializing temp dataset: ${this.config.tempDataset}`);
            
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const [exists] = await dataset.exists();
            
            if (!exists) {
                console.log(`Creating dataset: ${this.config.tempDataset}`);
                await dataset.create({
                    description: 'Temporary tables for JSON vs BigQuery validation',
                    location: 'US',
                    defaultTableExpirationMs: String(24 * 60 * 60 * 1000)
                });
                console.log(`Dataset created: ${this.config.tempDataset}`);
            } else {
                console.log(`Dataset already exists: ${this.config.tempDataset}`);
            }
            
            return {
                success: true,
                message: 'Temp dataset ready',
                datasetId: this.config.tempDataset
            };
            
        } catch (error) {
            console.error('Failed to initialize temp dataset:', error.message);
            throw new Error(`Dataset initialization failed: ${error.message}`);
        }
    }

    /**
     * FIXED: Create temp table preserving original field names + batch processing
     */
    async createTempTableFromJSON(jsonData, tableId, primaryKeyForVerification = null) {
        try {
            console.log(`FIXED: Creating temp table with PRESERVED CASE + BATCH PROCESSING`);
            console.log(`Input verification: ${jsonData.length} records to process`);
            
            // Verify input data integrity
            let inputPrimaryKeys = [];
            let uniqueInputIds = [];
            
            if (primaryKeyForVerification && primaryKeyForVerification !== 'undefined') {
                inputPrimaryKeys = jsonData.map(r => r[primaryKeyForVerification]).filter(id => id);
                uniqueInputIds = [...new Set(inputPrimaryKeys)];
                
                console.log(`INPUT VERIFICATION (using ${primaryKeyForVerification}):`);
                console.log(`   Total input records: ${jsonData.length}`);
                console.log(`   Records with ${primaryKeyForVerification}: ${inputPrimaryKeys.length}`);
                console.log(`   Unique ${primaryKeyForVerification} values: ${uniqueInputIds.length}`);
                console.log(`   Sample IDs: [${uniqueInputIds.slice(0, 5).join(', ')}...]`);
            }
            
            await this.initializeTempDataset();
            
            const timestamp = Date.now();
            const randomSuffix = Math.floor(Math.random() * 1000);
            const tempTableName = `${this.config.tempTablePrefix}${tableId}_${timestamp}_${randomSuffix}`;
            const fullTableId = `${process.env.GOOGLE_CLOUD_PROJECT_ID}.${this.config.tempDataset}.${tempTableName}`;
            
            console.log(`Using timestamped temp table: ${fullTableId}`);
            
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const table = dataset.table(tempTableName);
            
            // Process data with PRESERVED field names
            console.log(`Processing data with PRESERVED field names...`);
            
            const processedData = jsonData.map((record, index) => {
                const processed = {};
                
                const flattenObject = (obj, prefix = '') => {
                    for (const [key, value] of Object.entries(obj)) {
                        // FIXED: Use cleanFieldName but preserve case for simple fields
                        let cleanKey;
                        if (prefix === '' && /^[a-zA-Z][a-zA-Z0-9_]*$/.test(key)) {
                            // Simple field name - preserve original case
                            cleanKey = key;
                        } else {
                            // Complex field name - clean but try to preserve case
                            cleanKey = this.cleanFieldName(prefix ? `${prefix}_${key}` : key);
                        }
                        
                        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                            if (value.display_value !== undefined || value.link !== undefined || value.value !== undefined) {
                                if (value.display_value !== undefined && value.display_value !== null) {
                                    processed[`${cleanKey}_display_value`] = this.cleanDataValue(value.display_value, `${cleanKey}_display_value`);
                                }
                                if (value.link !== undefined && value.link !== null) {
                                    processed[`${cleanKey}_link`] = this.cleanDataValue(value.link, `${cleanKey}_link`);
                                }
                                if (value.value !== undefined && value.value !== null) {
                                    processed[`${cleanKey}_value`] = this.cleanDataValue(value.value, `${cleanKey}_value`);
                                }
                            } else {
                                if (prefix.split('_').length < 2) {
                                    flattenObject(value, cleanKey);
                                } else {
                                    processed[cleanKey] = this.cleanDataValue(JSON.stringify(value), cleanKey);
                                }
                            }
                        } else if (Array.isArray(value)) {
                            processed[cleanKey] = this.cleanDataValue(JSON.stringify(value), cleanKey);
                        } else {
                            processed[cleanKey] = this.cleanDataValue(value, cleanKey);
                        }
                    }
                };
                
                flattenObject(record);
                
                if (index === 0) {
                    console.log(`Processing verification (first record):`);
                    console.log(`   Original fields: ${Object.keys(record).length}`);
                    console.log(`   Processed fields: ${Object.keys(processed).length}`);
                    console.log(`   Original field names: [${Object.keys(record).slice(0, 5).join(', ')}]`);
                    console.log(`   Processed field names: [${Object.keys(processed).slice(0, 5).join(', ')}]`);
                    
                    if (primaryKeyForVerification) {
                        console.log(`   ${primaryKeyForVerification}: ${processed[primaryKeyForVerification] || 'NOT FOUND'}`);
                    }
                }
                
                return processed;
            });
            
            console.log(`Data processing complete: ${processedData.length} records processed`);

            const schema = this.generateCompleteSchemaFromProcessedData(processedData);
            console.log(`Schema generated: ${schema.length} fields`);
            console.log(`Schema field names: [${schema.slice(0, 10).map(f => f.name).join(', ')}]`);

            await table.create({
                schema: schema,
                description: `Temp table with preserved field names - ${new Date().toISOString()}`,
                expirationTime: Date.now() + 24 * 60 * 60 * 1000
            });
            
            console.log(`Fresh temp table created successfully`);
            console.log(`Waiting for BigQuery to initialize the table...`);
            await new Promise(resolve => setTimeout(resolve, 10000));
            
            const insertionData = processedData.map(record => {
                const cleanRecord = {};
                schema.forEach(field => {
                    const fieldName = field.name;
                    if (record.hasOwnProperty(fieldName)) {
                        const value = record[fieldName];
                        if (value === null || value === undefined || value === '') {
                            cleanRecord[fieldName] = null;
                        } else {
                            cleanRecord[fieldName] = String(value)
                                .replace(/[^\x20-\x7E\s]/g, '')
                                .trim()
                                .substring(0, 10000) || null;
                        }
                    } else {
                        cleanRecord[fieldName] = null;
                    }
                });
                return cleanRecord;
            });
            
            // Batch processing for large files
            const totalRecords = insertionData.length;
            const batchSize = this.config.maxBatchSize;
            
            if (totalRecords > batchSize) {
                console.log(`LARGE FILE DETECTED: ${totalRecords} records > ${batchSize} batch limit`);
                console.log(`BATCH PROCESSING: Breaking into ${Math.ceil(totalRecords / batchSize)} batches`);
                
                let totalInserted = 0;
                const batches = Math.ceil(totalRecords / batchSize);
                
                for (let i = 0; i < batches; i++) {
                    const start = i * batchSize;
                    const end = Math.min(start + batchSize, totalRecords);
                    const batch = insertionData.slice(start, end);
                    
                    console.log(`BATCH ${i + 1}/${batches}: Inserting records ${start + 1}-${end} (${batch.length} records)`);
                    
                    try {
                        const insertStartTime = Date.now();
                        await table.insert(batch);
                        const insertDuration = Date.now() - insertStartTime;
                        
                        totalInserted += batch.length;
                        console.log(`Batch ${i + 1} completed in ${insertDuration}ms (${totalInserted}/${totalRecords} total)`);
                        
                        if (i < batches - 1) {
                            await new Promise(resolve => setTimeout(resolve, 1000));
                        }
                        
                    } catch (batchError) {
                        console.error(`Batch ${i + 1} failed:`, batchError.message);
                        throw new Error(`BATCH INSERT FAILED at batch ${i + 1}: ${batchError.message}`);
                    }
                }
                
                console.log(`BATCH PROCESSING COMPLETED: ${totalInserted} total records inserted in ${batches} batches`);
                
            } else {
                console.log(`STANDARD PROCESSING: ${totalRecords} records <= ${batchSize} batch limit`);
                
                try {
                    const insertStartTime = Date.now();
                    console.log(`Starting SINGLE ATOMIC INSERT at ${new Date().toISOString()}`);
                    
                    await table.insert(insertionData);
                    
                    const insertDuration = Date.now() - insertStartTime;
                    console.log(`SINGLE ATOMIC INSERT COMPLETED in ${insertDuration}ms`);
                    
                } catch (insertError) {
                    console.error(`SINGLE ATOMIC INSERT FAILED:`, insertError.message);
                    throw new Error(`INSERT FAILED: ${insertError.message}`);
                }
            }
            
            // Verification with preserved field names
            console.log(`VERIFICATION: Using preserved field names...`);
            await new Promise(resolve => setTimeout(resolve, 6000));
            
            try {
                const basicVerificationQueries = [
                    {
                        name: 'Total Count',
                        query: `SELECT COUNT(*) as count FROM \`${fullTableId}\``
                    }
                ];
                
                // Only add primary key specific queries if we have a valid primary key
                if (primaryKeyForVerification && primaryKeyForVerification !== 'undefined') {
                    // Use original field name (preserved case)
                    basicVerificationQueries.push(
                        {
                            name: `Unique ${primaryKeyForVerification} Count`,
                            query: `SELECT COUNT(DISTINCT ${primaryKeyForVerification}) as count FROM \`${fullTableId}\``
                        },
                        {
                            name: `Non-null ${primaryKeyForVerification} Count`, 
                            query: `SELECT COUNT(*) as count FROM \`${fullTableId}\` WHERE ${primaryKeyForVerification} IS NOT NULL`
                        }
                    );
                }
                
                const verificationResults = {};
                
                for (const queryInfo of basicVerificationQueries) {
                    try {
                        const [queryResult] = await this.bigquery.query(queryInfo.query);
                        verificationResults[queryInfo.name] = queryResult;
                        const count = queryResult[0]?.count || 0;
                        console.log(`${queryInfo.name}: ${count}`);
                    } catch (verifyError) {
                        console.warn(`Verification query '${queryInfo.name}' failed: ${verifyError.message}`);
                        verificationResults[queryInfo.name] = { error: verifyError.message };
                    }
                }
                
                const finalCount = parseInt(verificationResults['Total Count'][0]?.count || 0);
                const uniqueCount = primaryKeyForVerification ? 
                    parseInt(verificationResults[`Unique ${primaryKeyForVerification} Count`]?.[0]?.count || 0) : 
                    'N/A';
                
                console.log(`VERIFICATION RESULTS:`);
                console.log(`   Original JSON records: ${jsonData.length}`);
                console.log(`   Records in temp table: ${finalCount}`);
                console.log(`   Unique primary key values: ${uniqueCount}`);
                
                if (finalCount === 0) {
                    throw new Error('ZERO RECORDS: No records were successfully inserted into temp table');
                }
                
                if (finalCount > jsonData.length) {
                    throw new Error(`DUPLICATION ERROR: Expected ${jsonData.length} records, found ${finalCount}`);
                }
                
                console.log(`VERIFICATION PASSED: Records processed successfully with preserved field names`);
                
                return {
                    success: true,
                    message: 'SUCCESS: Temp table created with preserved field names + batch processing',
                    tempTableId: fullTableId,
                    tempTableName: tempTableName,
                    inputRecords: jsonData.length,
                    recordsInTable: finalCount,
                    uniqueIdsInput: uniqueInputIds.length,
                    uniqueIdsInTable: uniqueCount !== 'N/A' ? uniqueCount : 'Unknown',
                    recordCountMatch: finalCount === jsonData.length,
                    uniqueCountMatch: uniqueCount !== 'N/A' ? uniqueCount === uniqueInputIds.length : true,
                    verification: verificationResults,
                    fieldsProcessed: schema.length,
                    approach: totalRecords > batchSize ? `batch-processing-${Math.ceil(totalRecords / batchSize)}-batches` : 'single-insert',
                    batchInfo: totalRecords > batchSize ? {
                        totalBatches: Math.ceil(totalRecords / batchSize),
                        batchSize: batchSize,
                        largeFileHandling: true
                    } : null,
                    expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
                    fixes: ['Preserved original field names for proper matching', 'Batch processing for large files', 'Dynamic table support']
                };
                
            } catch (verifyError) {
                console.error(`Verification failed:`, verifyError.message);
                throw new Error(`Verification failed: ${verifyError.message}`);
            }
            
        } catch (error) {
            console.error('Temp table creation failed:', error.message);
            throw new Error(`Temp table creation failed: ${error.message}`);
        }
    }

    generateCompleteSchemaFromProcessedData(processedData) {
        const allFields = new Set();
        
        processedData.forEach((record) => {
            Object.keys(record).forEach(key => {
                allFields.add(key);
            });
        });
        
        const schema = Array.from(allFields).sort().map(fieldName => {
            return {
                name: fieldName,  // Preserve original case
                type: 'STRING',
                mode: 'NULLABLE'
            };
        });
        
        console.log(`Complete schema: ${schema.length} fields with preserved names`);
        
        // Show potential key fields
        const potentialKeys = schema.filter(f => 
            f.name.toLowerCase().includes('id') || 
            f.name.toLowerCase().includes('key') || 
            f.name.toLowerCase().includes('number') ||
            f.name.toLowerCase().includes('arn')
        ).map(f => f.name);
        console.log(`Potential key fields: [${potentialKeys.slice(0, 10).join(', ')}]`);
        
        return schema;
    }

    async cleanupTempTable(tempTableName) {
        try {
            console.log(`Cleaning up temp table: ${tempTableName}`);
            
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const table = dataset.table(tempTableName);
            
            const [exists] = await table.exists();
            if (exists) {
                await table.delete();
                console.log(`Temp table deleted: ${tempTableName}`);
                return { success: true, message: 'Temp table cleaned up' };
            } else {
                console.log(`Temp table not found: ${tempTableName}`);
                return { success: true, message: 'Temp table not found (may have expired)' };
            }
            
        } catch (error) {
            console.error('Failed to cleanup temp table:', error.message);
            throw new Error(`Cleanup failed: ${error.message}`);
        }
    }
}

module.exports = BigQueryIntegrationService;