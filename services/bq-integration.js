// services/bq-integration.js - COMPLETE UNIVERSAL VERSION
const { BigQuery } = require('@google-cloud/bigquery');

class BigQueryIntegrationService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        this.dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET || 'stage_three_dw');

        console.log('BigQuery Integration Service - UNIVERSAL API SUPPORT');
        console.log('Works with: ANY REST API, GraphQL, JSON responses, nested objects');
    }

    async createTempTableFromJSON(jsonData, tableSuffix, primaryKey) {
        try {
            const tempTableId = `universal_api_${this.sanitizeTableName(tableSuffix)}_${Date.now()}`;

            console.log(`Creating universal temp table: ${tempTableId}`);
            console.log(`Processing ${jsonData.length} records from any API format...`);

            if (jsonData.length === 0) {
                throw new Error('No API data provided');
            }

            await this.dataset.get({ autoCreate: true });

            // UNIVERSAL: Process any API response structure
            const processedData = this.processUniversalAPIData(jsonData);
            console.log(`Universal processing completed: ${processedData.length} records ready`);

            if (processedData.length === 0) {
                throw new Error('No valid records after universal processing');
            }

            // UNIVERSAL: Create ALL STRING schema to avoid type issues
            const schema = this.createUniversalStringSchema(processedData[0]);
            console.log(`Universal schema created with ${schema.length} fields (all strings for compatibility)`);

            // Create table with universal schema
            const [table] = await this.dataset.createTable(tempTableId, {
                schema: schema,
                expirationMs: 3600000,
                location: 'US'
            });

            console.log('Universal table created, inserting processed data...');

            // UNIVERSAL: Convert all data to strings for guaranteed insertion
            const stringData = this.convertToAllStrings(processedData);

            // Insert with error handling
            let insertResult = { strategy: 'unknown', successCount: 0 };

            try {
                await table.insert(stringData, {
                    ignoreUnknownValues: false,
                    skipInvalidRows: false
                });
                insertResult = { strategy: 'bulk_all_strings', successCount: stringData.length };
                console.log('UNIVERSAL: Bulk insert successful');

            } catch (bulkError) {
                console.log('UNIVERSAL: Bulk insert failed, trying individual insertion...');

                let successCount = 0;
                for (let i = 0; i < stringData.length; i++) {
                    try {
                        await table.insert([stringData[i]]);
                        successCount++;
                    } catch (recordError) {
                        console.error(`Record ${i + 1} failed:`, recordError.message);
                    }
                }

                insertResult = { strategy: 'individual_fallback', successCount: successCount };
                console.log(`UNIVERSAL: Individual insertion completed: ${successCount}/${stringData.length} successful`);
            }

            // Verify insertion
            const fullTableId = `${this.bigquery.projectId}.${this.dataset.id}.${tempTableId}`;
            const recordCount = await this.verifyInsertion(fullTableId);

            console.log(`UNIVERSAL PROCESSING COMPLETE: ${recordCount} records verified in BigQuery`);

            return {
                success: true,
                message: `Universal API temp table created with ${recordCount} records`,
                tempTableId: fullTableId,
                tempTableName: tempTableId,
                recordsInTable: recordCount,
                inputRecords: jsonData.length,
                processedRecords: processedData.length,
                recordCountMatch: recordCount === processedData.length,
                primaryKeyField: primaryKey,
                schemaType: 'UNIVERSAL_ALL_STRINGS',
                insertionStrategy: insertResult.strategy,
                universalSupport: true,
                expiresAt: new Date(Date.now() + 3600000).toISOString()
            };

        } catch (error) {
            console.error('Universal API processing failed:', error);
            throw new Error(`Universal API integration failed: ${error.message}`);
        }
    }

    // UNIVERSAL: Process any API data structure
    processUniversalAPIData(rawData) {
        console.log('UNIVERSAL: Processing API data from any source...');

        const dataArray = Array.isArray(rawData) ? rawData : [rawData];
        console.log(`Input: ${dataArray.length} records to process`);

        return dataArray.map((record, index) => {
            if (!record || typeof record !== 'object') {
                console.warn(`Skipping invalid record ${index + 1}:`, typeof record);
                return null;
            }

            return this.flattenUniversalRecord(record);
        }).filter(record => record !== null);
    }

    // UNIVERSAL: Flatten any record structure
    flattenUniversalRecord(record, prefix = '', maxDepth = 4, currentDepth = 0) {
        const flattened = {};

        if (currentDepth >= maxDepth) {
            const fieldName = this.sanitizeFieldName(prefix || 'data');
            flattened[fieldName] = JSON.stringify(record);
            return flattened;
        }

        Object.entries(record).forEach(([key, value]) => {
            const fieldName = this.sanitizeFieldName(prefix ? `${prefix}_${key}` : key);

            if (value === null || value === undefined) {
                flattened[fieldName] = null;
            } else if (Array.isArray(value)) {
                // Convert arrays to JSON strings and add count
                flattened[fieldName] = JSON.stringify(value);
                flattened[`${fieldName}_count`] = value.length;
            } else if (typeof value === 'object') {
                // Handle nested objects based on complexity
                const objectKeys = Object.keys(value);

                if (objectKeys.length <= 3 && this.isSimpleReference(value)) {
                    // Simple reference object - extract key fields
                    this.extractReferenceFields(value, fieldName, flattened);
                } else {
                    // Complex object - flatten recursively
                    const nested = this.flattenUniversalRecord(value, fieldName, maxDepth, currentDepth + 1);
                    Object.assign(flattened, nested);
                }
            } else {
                // Simple values - keep as is
                flattened[fieldName] = value;
            }
        });

        return flattened;
    }

    // UNIVERSAL: Detect simple reference objects (common in APIs)
    isSimpleReference(obj) {
        const keys = Object.keys(obj);
        const referencePatterns = ['id', 'name', 'value', 'display_value', 'link', 'href', 'url'];
        return keys.length <= 4 && keys.some(key =>
            referencePatterns.some(pattern => key.toLowerCase().includes(pattern))
        );
    }

    // UNIVERSAL: Extract reference fields
    extractReferenceFields(obj, fieldName, flattened) {
        Object.entries(obj).forEach(([key, value]) => {
            const cleanKey = this.sanitizeFieldName(`${fieldName}_${key}`);
            flattened[cleanKey] = value !== null ? String(value) : null;
        });
    }

    // UNIVERSAL: Create all-string schema for any data structure
    createUniversalStringSchema(sampleRecord) {
        const schema = [];

        Object.keys(sampleRecord).forEach(fieldName => {
            schema.push({
                name: fieldName,
                type: 'STRING', // Use STRING for all fields to avoid type conflicts
                mode: 'NULLABLE'
            });
        });

        console.log(`Universal string schema: ${schema.length} fields (all STRING type for compatibility)`);
        return schema;
    }

    // UNIVERSAL: Convert all data to strings for guaranteed BigQuery insertion
    convertToAllStrings(processedData) {
        return processedData.map(record => {
            const stringRecord = {};

            Object.entries(record).forEach(([key, value]) => {
                if (value === null || value === undefined) {
                    stringRecord[key] = null;
                } else {
                    stringRecord[key] = String(value);
                }
            });

            return stringRecord;
        });
    }

    // UNIVERSAL: Verify insertion with retries
    async verifyInsertion(tableId, maxAttempts = 5) {
        let recordCount = 0;

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            await new Promise(resolve => setTimeout(resolve, 1000 * attempt));

            try {
                const [countResult] = await this.bigquery.query(
                    `SELECT COUNT(*) as count FROM \`${tableId}\``
                );
                recordCount = parseInt(countResult[0].count);

                console.log(`Verification attempt ${attempt}: ${recordCount} records`);

                if (recordCount > 0) break;

            } catch (countError) {
                console.error(`Verification attempt ${attempt} failed:`, countError.message);
            }
        }

        return recordCount;
    }

    // UNIVERSAL: Sanitize any table name
    sanitizeTableName(name) {
        return String(name)
            .replace(/[^a-zA-Z0-9_]/g, '_')
            .replace(/^[0-9]/, '_$&')
            .substring(0, 50);
    }

    // UNIVERSAL: Sanitize any field name
    sanitizeFieldName(name) {
        return String(name)
            .replace(/[^a-zA-Z0-9_]/g, '_')
            .replace(/^[0-9]/, '_$&')
            .toLowerCase()
            .substring(0, 128);
    }

    async testConnection() {
        try {
            const [datasets] = await this.bigquery.getDatasets({ maxResults: 1 });
            return {
                success: true,
                message: 'BigQuery connection successful (Universal API Support)',
                projectId: this.bigquery.projectId,
                universalAPISupport: true
            };
        } catch (error) {
            return {
                success: false,
                error: error.message,
                suggestions: [
                    'Check GOOGLE_CLOUD_PROJECT_ID in .env file',
                    'Verify BigQuery API is enabled',
                    'Ensure authentication is configured'
                ]
            };
        }
    }

    async testSourceTableAccess(tableName) {
        try {
            if (!tableName) return { success: false, error: 'Table name required' };

            const [rows] = await this.bigquery.query(`SELECT COUNT(*) as row_count FROM \`${tableName}\` LIMIT 1`);
            return {
                success: true,
                message: `Table accessible for universal comparison`,
                rowCount: rows[0].row_count
            };
        } catch (error) {
            return {
                success: false,
                error: error.message,
                tableName: tableName
            };
        }
    }

    async cleanupTempTable(tableId) {
        try {
            const parts = tableId.split('.');
            const tableName = parts[parts.length - 1];
            await this.dataset.table(tableName).delete();
            console.log(`Cleaned up universal temp table: ${tableName}`);
        } catch (error) {
            console.warn('Universal cleanup failed:', error.message);
        }
    }

    // UNIVERSAL: Convert any data value to BigQuery-safe format
    convertUniversalValue(value, forceString = true) {
        if (value === null || value === undefined) return null;

        if (forceString) {
            // Force everything to string for maximum compatibility
            return String(value)
                .replace(/\0/g, '')
                .replace(/[\x00-\x08\x0B-\x1F\x7F]/g, ' ')
                .trim()
                .substring(0, 50000);
        }

        // Type-aware conversion (use with caution)
        if (typeof value === 'string') {
            const trimmed = value.trim();

            // Boolean detection
            if (['true', 'false'].includes(trimmed.toLowerCase())) {
                return trimmed.toLowerCase() === 'true';
            }

            // Number detection
            if (/^-?\d+$/.test(trimmed)) {
                const num = parseInt(trimmed);
                return isNaN(num) ? trimmed : num;
            }

            if (/^-?\d*\.\d+$/.test(trimmed)) {
                const num = parseFloat(trimmed);
                return isNaN(num) ? trimmed : num;
            }

            return trimmed.substring(0, 50000);
        }

        return value;
    }

    // UNIVERSAL: Save processed API data as JSON file (for manual upload)
    async saveUniversalJSONFile(apiData, outputName = null) {
        try {
            const fs = require('fs').promises;
            const path = require('path');

            const processedData = this.processUniversalAPIData(apiData);
            const filename = outputName || `universal_api_${Date.now()}.json`;
            const outputPath = path.join(__dirname, '..', 'downloads', filename);

            // Ensure directory exists
            await fs.mkdir(path.dirname(outputPath), { recursive: true });

            await fs.writeFile(outputPath, JSON.stringify(processedData, null, 2));

            console.log(`Universal JSON saved: ${outputPath}`);
            console.log(`Records: ${processedData.length}`);

            return {
                success: true,
                filePath: outputPath,
                filename: filename,
                recordCount: processedData.length
            };

        } catch (error) {
            console.error('Universal JSON save failed:', error);
            throw error;
        }
    }
}

module.exports = BigQueryIntegrationService;