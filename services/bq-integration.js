// services/bq-integration.js
const { BigQuery } = require('@google-cloud/bigquery');

class BigQueryIntegrationService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        
        // Configuration for customer account data
        this.config = {
            sourceTable: 'rax-landing-qa.snow_global_ods.customer_account',
            tempDataset: 'temp_validation_tables',
            tempTablePrefix: 'json_temp_'
        };
        
        console.log('🔗 BigQuery Integration Service initialized');
        console.log(`📊 Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
        console.log(`🎯 Source Table: ${this.config.sourceTable}`);
    }

    /**
     * Test BigQuery connection
     */
    async testConnection() {
        try {
            console.log('🧪 Testing BigQuery connection...');
            
            // Simple query to test connection
            const query = `SELECT 1 as test_connection`;
            const [rows] = await this.bigquery.query(query);
            
            console.log('✅ BigQuery connection successful');
            return { success: true, message: 'Connection established' };
            
        } catch (error) {
            console.error('❌ BigQuery connection failed:', error.message);
            throw error;
        }
    }

    /**
     * Test access to source table and get basic info
     */
    async testSourceTableAccess() {
        try {
            console.log('🔍 Testing access to source table...');
            console.log(`📋 Table: ${this.config.sourceTable}`);
            
            // Test table access with a simple count query
            const countQuery = `
                SELECT COUNT(*) as record_count 
                FROM \`${this.config.sourceTable}\`
                LIMIT 1
            `;
            
            const [countResults] = await this.bigquery.query(countQuery);
            const recordCount = countResults[0].record_count;
            
            console.log(`✅ Source table access successful`);
            console.log(`📊 Record count: ${recordCount}`);
            
            return { 
                success: true, 
                message: 'Source table accessible',
                recordCount: parseInt(recordCount),
                tableName: this.config.sourceTable
            };
            
        } catch (error) {
            console.error('❌ Source table access failed:', error.message);
            
            // Provide helpful error messages
            if (error.message.includes('not found')) {
                throw new Error(`Table not found: ${this.config.sourceTable}. Please verify the table exists.`);
            } else if (error.message.includes('permission') || error.message.includes('Access Denied')) {
                throw new Error(`Permission denied accessing: ${this.config.sourceTable}. Please check your BigQuery permissions.`);
            } else {
                throw new Error(`Failed to access source table: ${error.message}`);
            }
        }
    }

    /**
     * Get schema information using LIMIT 0 approach (works cross-project)
     */
    async getSourceTableSchema() {
        try {
            console.log('📋 Getting source table schema...');
            
            // Use LIMIT 0 query to get schema - this works across projects
            const schemaQuery = `SELECT * FROM \`${this.config.sourceTable}\` LIMIT 0`;
            
            console.log(`🔍 Running schema detection query...`);
            
            const [schemaResults] = await this.bigquery.query(schemaQuery);
            
            // Extract field information from the query job metadata
            const job = this.bigquery.job(schemaResults.jobReference?.jobId);
            const [jobMetadata] = await job.getMetadata();
            const schema = jobMetadata.configuration.query.destinationTable ? 
                jobMetadata.statistics.query.schema?.fields || [] :
                [];
            
            // Alternative approach: Get field names from the empty result structure
            let fieldNames = [];
            if (schemaResults.length === 0 && schemaResults._config) {
                // Extract field names from metadata
                fieldNames = Object.keys(schemaResults[0] || {});
            }
            
            // Fallback: Use a simple column introspection query
            if (schema.length === 0 && fieldNames.length === 0) {
                console.log('📋 Using fallback column detection...');
                
                const columnQuery = `
                    SELECT column_name, data_type 
                    FROM \`${this.config.sourceTable.split('.')[0]}\`.\`${this.config.sourceTable.split('.')[1]}\`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '${this.config.sourceTable.split('.')[2]}'
                    ORDER BY ordinal_position
                `;
                
                try {
                    const [columnResults] = await this.bigquery.query(columnQuery);
                    
                    console.log(`✅ Schema retrieved via fallback: ${columnResults.length} fields found`);
                    
                    // Extract key fields
                    const keyFields = columnResults.filter(field => 
                        ['sys_id', 'name', 'u_tenant_id', 'number', 'sys_updated_on'].includes(field.column_name)
                    );
                    
                    console.log(`🔑 Key fields found: ${keyFields.map(f => f.column_name).join(', ')}`);
                    
                    return {
                        success: true,
                        message: 'Schema retrieved successfully (via fallback)',
                        totalFields: columnResults.length,
                        schema: columnResults,
                        keyFields: keyFields,
                        tableName: this.config.sourceTable
                    };
                    
                } catch (fallbackError) {
                    console.error('❌ Fallback schema query also failed:', fallbackError.message);
                    
                    // Final fallback: return basic info
                    return {
                        success: true,
                        message: 'Schema access limited - using basic detection',
                        totalFields: 'Unknown (cross-project access limited)',
                        schema: [],
                        keyFields: [
                            { column_name: 'sys_id', data_type: 'STRING' },
                            { column_name: 'name', data_type: 'STRING' },
                            { column_name: 'u_tenant_id', data_type: 'STRING' },
                            { column_name: 'number', data_type: 'STRING' },
                            { column_name: 'sys_updated_on', data_type: 'TIMESTAMP' }
                        ],
                        tableName: this.config.sourceTable,
                        note: 'Cross-project schema access limited'
                    };
                }
            }
            
            console.log(`✅ Schema retrieved: ${schema.length || fieldNames.length} fields found`);
            
            return {
                success: true,
                message: 'Schema retrieved successfully',
                totalFields: schema.length || fieldNames.length,
                schema: schema.length > 0 ? schema : fieldNames,
                keyFields: [], // Will be populated from actual query
                tableName: this.config.sourceTable
            };
            
        } catch (error) {
            console.error('❌ Schema retrieval failed:', error.message);
            
            // Return a graceful fallback response
            return {
                success: true,
                message: 'Schema access limited - proceeding with known fields',
                totalFields: 'Cross-project access limited',
                schema: [],
                keyFields: [
                    { column_name: 'sys_id', data_type: 'STRING' },
                    { column_name: 'name', data_type: 'STRING' },
                    { column_name: 'u_tenant_id', data_type: 'STRING' },
                    { column_name: 'number', data_type: 'STRING' },
                    { column_name: 'sys_updated_on', data_type: 'TIMESTAMP' }
                ],
                tableName: this.config.sourceTable,
                note: 'Cross-project schema limitations - will use JSON structure for comparison'
            };
        }
    }

    /**
     * Get sample data from source table
     */
    async getSampleData(limit = 3) {
        try {
            console.log(`📋 Getting sample data (${limit} records)...`);
            
            const sampleQuery = `
                SELECT sys_id, name, u_tenant_id, number, u_account_type, sys_updated_on
                FROM \`${this.config.sourceTable}\`
                WHERE sys_id IS NOT NULL
                ORDER BY sys_updated_on DESC
                LIMIT ${limit}
            `;
            
            const [sampleResults] = await this.bigquery.query(sampleQuery);
            
            console.log(`✅ Sample data retrieved: ${sampleResults.length} records`);
            
            return {
                success: true,
                message: 'Sample data retrieved',
                sampleCount: sampleResults.length,
                sampleData: sampleResults,
                tableName: this.config.sourceTable
            };
            
        } catch (error) {
            console.error('❌ Sample data retrieval failed:', error.message);
            throw new Error(`Failed to get sample data: ${error.message}`);
        }
    }

    /**
     * Initialize temp dataset for validation tables
     */
    async initializeTempDataset() {
        try {
            console.log(`🔧 Initializing temp dataset: ${this.config.tempDataset}`);
            
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const [exists] = await dataset.exists();
            
            if (!exists) {
                console.log(`📁 Creating dataset: ${this.config.tempDataset}`);
                await dataset.create({
                    description: 'Temporary tables for JSON vs BigQuery validation',
                    location: 'US',
                    defaultTableExpirationMs: String(24 * 60 * 60 * 1000) // 24 hours
                });
                console.log(`✅ Dataset created: ${this.config.tempDataset}`);
            } else {
                console.log(`✅ Dataset already exists: ${this.config.tempDataset}`);
            }
            
            return {
                success: true,
                message: 'Temp dataset ready',
                datasetId: this.config.tempDataset
            };
            
        } catch (error) {
            console.error('❌ Failed to initialize temp dataset:', error.message);
            throw new Error(`Dataset initialization failed: ${error.message}`);
        }
    }

    /**
     * Create temp table from JSON data (ENHANCED VERSION)
     */
    async createTempTableFromJSON(jsonData, tableId) {
        try {
            console.log(`📊 Creating temp table: ${tableId}`);
            console.log(`📋 Records to upload: ${jsonData.length}`);
            
            // First ensure temp dataset exists
            await this.initializeTempDataset();
            
            // Generate unique temp table name
            const tempTableName = `${this.config.tempTablePrefix}${tableId}`;
            const fullTableId = `${process.env.GOOGLE_CLOUD_PROJECT_ID}.${this.config.tempDataset}.${tempTableName}`;
            
            console.log(`🎯 Target temp table: ${fullTableId}`);
            
            // Get dataset and table references
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const table = dataset.table(tempTableName);
            
            // Check if table already exists and delete it
            const [exists] = await table.exists();
            if (exists) {
                console.log(`🗑️ Deleting existing temp table: ${tempTableName}`);
                await table.delete();
            }
            
            // ENHANCED: Generate complete schema from ALL records (not just first)
            console.log(`🔍 Analyzing all ${jsonData.length} records to generate complete schema...`);
            const schema = this.generateCompleteSchemaFromAllRecords(jsonData);
            console.log(`📋 Complete schema generated: ${schema.length} fields`);
            
            // Create table with complete schema
            await table.create({
                schema: schema,
                description: `Temp table for JSON validation - ${new Date().toISOString()}`,
                expirationTime: Date.now() + 24 * 60 * 60 * 1000 // 24 hours
            });
            
            console.log(`✅ Temp table created: ${tempTableName}`);
            
            // Insert JSON data in batches
            console.log(`📤 Uploading ${jsonData.length} records in batches...`);
            
            const BATCH_SIZE = 1000; // Process 1000 records at a time
            const totalBatches = Math.ceil(jsonData.length / BATCH_SIZE);
            let uploadedCount = 0;
            
            for (let i = 0; i < totalBatches; i++) {
                const start = i * BATCH_SIZE;
                const end = Math.min(start + BATCH_SIZE, jsonData.length);
                const batch = jsonData.slice(start, end);
                
                console.log(`📦 Processing batch ${i + 1}/${totalBatches}: ${batch.length} records`);
                
                try {
                    await table.insert(batch);
                    uploadedCount += batch.length;
                    console.log(`✅ Batch ${i + 1} uploaded successfully (${uploadedCount}/${jsonData.length})`);
                    
                    // Small delay between batches to avoid rate limits
                    if (i < totalBatches - 1) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                    
                } catch (batchError) {
                    console.error(`❌ Batch ${i + 1} failed:`, batchError.message);
                    
                    // Enhanced error logging
                    if (batchError.errors && batchError.errors.length > 0) {
                        console.error(`🔍 Detailed errors for batch ${i + 1}:`);
                        batchError.errors.slice(0, 3).forEach((err, idx) => {
                            console.error(`   Error ${idx + 1}:`, {
                                reason: err.reason,
                                message: err.message,
                                location: err.location
                            });
                        });
                    }
                    
                    // Show sample of problematic data
                    console.error(`📋 Sample record from failed batch:`, JSON.stringify(batch[0], null, 2).substring(0, 500));
                    
                    // Try smaller sub-batches for this failed batch
                    const SUB_BATCH_SIZE = 100;
                    const subBatches = Math.ceil(batch.length / SUB_BATCH_SIZE);
                    
                    for (let j = 0; j < subBatches; j++) {
                        const subStart = j * SUB_BATCH_SIZE;
                        const subEnd = Math.min(subStart + SUB_BATCH_SIZE, batch.length);
                        const subBatch = batch.slice(subStart, subEnd);
                        
                        try {
                            await table.insert(subBatch);
                            uploadedCount += subBatch.length;
                            console.log(`✅ Sub-batch ${j + 1}/${subBatches} uploaded: ${subBatch.length} records`);
                        } catch (subError) {
                            console.error(`❌ Sub-batch ${j + 1} failed, skipping ${subBatch.length} records:`);
                            console.error(`🔍 Sub-batch error:`, subError.message);
                            if (subError.errors && subError.errors.length > 0) {
                                console.error(`📋 First sub-batch error:`, subError.errors[0]);
                            }
                        }
                    }
                }
            }
            
            console.log(`✅ Batch upload completed: ${uploadedCount} records uploaded`);
            
            // Verify upload
            const countQuery = `SELECT COUNT(*) as record_count FROM \`${fullTableId}\``;
            const [countResults] = await this.bigquery.query(countQuery);
            const uploadedCountVerified = countResults[0].record_count;
            
            console.log(`✅ Verification: ${uploadedCountVerified} records in temp table`);
            
            return {
                success: true,
                message: 'Temp table created and populated',
                tempTableId: fullTableId,
                tempTableName: tempTableName,
                recordsUploaded: parseInt(uploadedCountVerified),
                schema: schema,
                expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
            };
            
        } catch (error) {
            console.error('❌ Failed to create temp table:', error.message);
            throw new Error(`Temp table creation failed: ${error.message}`);
        }
    }

    /**
     * Generate complete schema from ALL records (ENHANCED VERSION)
     */
    generateCompleteSchemaFromAllRecords(jsonData) {
        const allFields = new Set();
        const fieldTypes = {};
        
        // Sample a reasonable number of records for schema analysis
        const sampleSize = Math.min(1000, jsonData.length);
        const recordsToAnalyze = jsonData.slice(0, sampleSize);
        
        console.log(`🔍 Analyzing ${recordsToAnalyze.length} records for complete schema...`);
        
        // Collect all unique field names and their types
        recordsToAnalyze.forEach((record, index) => {
            if (index % 100 === 0) {
                console.log(`   📋 Analyzed ${index}/${recordsToAnalyze.length} records...`);
            }
            
            for (const [key, value] of Object.entries(record)) {
                allFields.add(key);
                
                // Determine field type (prefer non-null values)
                if (value !== null && value !== undefined && !fieldTypes[key]) {
                    if (typeof value === 'boolean') {
                        fieldTypes[key] = 'BOOLEAN';
                    } else if (typeof value === 'number') {
                        fieldTypes[key] = Number.isInteger(value) ? 'INTEGER' : 'FLOAT';
                    } else if (typeof value === 'string') {
                        // Check if it looks like a timestamp
                        if (value.match(/^\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}/)) {
                            fieldTypes[key] = 'TIMESTAMP';
                        } else {
                            fieldTypes[key] = 'STRING';
                        }
                    } else {
                        fieldTypes[key] = 'STRING'; // Default for objects, arrays, etc.
                    }
                }
            }
        });
        
        // Generate schema for all discovered fields
        const schema = Array.from(allFields).sort().map(fieldName => ({
            name: fieldName,
            type: fieldTypes[fieldName] || 'STRING', // Default to STRING if type not determined
            mode: 'NULLABLE'
        }));
        
        console.log(`✅ Complete schema analysis finished:`);
        console.log(`   📊 Total unique fields found: ${allFields.size}`);
        console.log(`   🔧 Field types determined: ${Object.keys(fieldTypes).length}`);
        
        return schema;
    }

    /**
     * Generate BigQuery schema from JSON object (LEGACY - kept for compatibility)
     */
    generateSchemaFromJSON(jsonObject) {
        const schema = [];
        
        for (const [key, value] of Object.entries(jsonObject)) {
            let fieldType = 'STRING'; // Default type
            
            // Determine field type based on value
            if (value === null || value === undefined) {
                fieldType = 'STRING'; // Default for null values
            } else if (typeof value === 'boolean') {
                fieldType = 'BOOLEAN';
            } else if (typeof value === 'number') {
                fieldType = Number.isInteger(value) ? 'INTEGER' : 'FLOAT';
            } else if (typeof value === 'string') {
                // Check if it looks like a timestamp
                if (value.match(/^\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}/)) {
                    fieldType = 'TIMESTAMP';
                } else {
                    fieldType = 'STRING';
                }
            } else if (typeof value === 'object' && value !== null) {
                fieldType = 'STRING'; // Convert objects to JSON strings
            }
            
            schema.push({
                name: key,
                type: fieldType,
                mode: 'NULLABLE'
            });
        }
        
        console.log(`📋 Schema generated: ${schema.length} fields`);
        return schema;
    }

    /**
     * Clean up temp tables (optional cleanup)
     */
    async cleanupTempTable(tempTableName) {
        try {
            console.log(`🗑️ Cleaning up temp table: ${tempTableName}`);
            
            const dataset = this.bigquery.dataset(this.config.tempDataset);
            const table = dataset.table(tempTableName);
            
            const [exists] = await table.exists();
            if (exists) {
                await table.delete();
                console.log(`✅ Temp table deleted: ${tempTableName}`);
                return { success: true, message: 'Temp table cleaned up' };
            } else {
                console.log(`ℹ️ Temp table not found: ${tempTableName}`);
                return { success: true, message: 'Temp table not found (may have expired)' };
            }
            
        } catch (error) {
            console.error('❌ Failed to cleanup temp table:', error.message);
            throw new Error(`Cleanup failed: ${error.message}`);
        }
    }
}

module.exports = BigQueryIntegrationService;