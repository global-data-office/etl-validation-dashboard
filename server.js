// server.js - UNIVERSAL DATA TYPES + DUAL DUPLICATES ANALYSIS + EXCEL EXPORT READY
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const jsonUploadRouter = require('./routes/json-upload');
const BigQueryIntegrationService = require('./services/bq-integration');
const RDBMSIntegrationService = require('./services/rdbms-integration');
const RDBMSComparisonEngineService = require('./services/rdbms-comparison-engine'); // NEW: RDBMS-specific comparison engine
const APIFetcherService = require('./services/api-fetcher');
require('dotenv').config();

// âœ… ADD ORACLE THICK MODE HERE (lines 11-19)
const oracledb = require('oracledb');
try {
    oracledb.initOracleClient({ libDir: 'C:\\oracle\\instantclient_19_29' });
    console.log('âœ… Oracle Thick Mode initialized');
} catch (err) {
    if (!err.message.includes('already been called')) {
        console.error('âŒ Oracle init failed:', err.message);
    }
}
const app = express();
const port = process.env.PORT || 3000;
const apiFetcher = new APIFetcherService();

// Helper: Expand UUID-keyed or numeric-keyed objects into row arrays with snake_case fields
function camelToSnake(field) {
    return field
        .replace(/([A-Z])/g, '_$1')
        .toLowerCase()
        .replace(/^_/, ''); // Remove leading underscore if field started with uppercase
}

function expandUUIDKeyedData(data) {
    if (Array.isArray(data)) {
        // Even if it's already an array, apply camelCase to snake_case conversion
        return data.map(record => {
            const converted = {};
            for (const [field, value] of Object.entries(record)) {
                converted[camelToSnake(field)] = value;
            }
            return converted;
        });
    }
    
    const keys = Object.keys(data || {});
    if (keys.length === 0) return [data];
    
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const isUUIDKeyed = keys.slice(0, 5).every(k => uuidPattern.test(k));
    const isNumericKeyed = keys.slice(0, 5).every(k => /^\d+$/.test(k));
    
    if (isUUIDKeyed) {
        console.log(`Detected UUID-keyed object with ${keys.length} keys, expanding to rows...`);
        const rows = [];
        for (const [uuid, entries] of Object.entries(data)) {
            if (Array.isArray(entries)) {
                for (const entry of entries) {
                    const row = { uuid };
                    for (const [field, value] of Object.entries(entry)) {
                        row[camelToSnake(field)] = value;
                    }
                    rows.push(row);
                }
            } else if (typeof entries === 'object' && entries !== null) {
                const row = { uuid };
                for (const [field, value] of Object.entries(entries)) {
                    row[camelToSnake(field)] = value;
                }
                rows.push(row);
            }
        }
        console.log(`Expanded UUID-keyed to ${rows.length} rows`);
        return rows;
    } else if (isNumericKeyed) {
        // Numeric keys mean it's an array-like object â€” convert values to rows
        console.log(`Detected numeric-keyed object with ${keys.length} keys, converting to array...`);
        const rows = [];
        for (const entry of Object.values(data)) {
            if (typeof entry === 'object' && entry !== null) {
                const row = {};
                for (const [field, value] of Object.entries(entry)) {
                    row[camelToSnake(field)] = value;
                }
                rows.push(row);
            }
        }
        console.log(`Converted numeric-keyed to ${rows.length} rows`);
        return rows;
    }
    
    // Single object, still convert camelCase
    const converted = {};
    for (const [field, value] of Object.entries(data)) {
        converted[camelToSnake(field)] = value;
    }
    return [converted];
}

// Helper: Parse Azure Application Insights tabular format (tables[].columns + tables[].rows)
// Converts { tables: [{ columns: [{name, type}], rows: [[...], ...] }] } into flat row objects
function parseTabularFormat(data) {
    if (!data || typeof data !== 'object') return null;

    // Detect the tabular format: must have a 'tables' array with columns and rows
    const tables = data.tables || data.Tables;
    if (!Array.isArray(tables) || tables.length === 0) return null;

    const table = tables[0]; // Use the first (usually "PrimaryResult") table
    const columns = table.columns || table.Columns;
    const rows = table.rows || table.Rows;

    if (!Array.isArray(columns) || !Array.isArray(rows)) return null;
    if (columns.length === 0 || rows.length === 0) return null;

    // Validate it looks like a proper tabular response (columns have name property)
    if (!columns[0].name && !columns[0].Name) return null;

    console.log(`=== DETECTED AZURE APP INSIGHTS TABULAR FORMAT ===`);
    console.log(`Table: ${table.name || 'PrimaryResult'}`);
    console.log(`Columns: ${columns.length}`);
    console.log(`Rows: ${rows.length}`);
    console.log(`Column names: [${columns.slice(0, 10).map(c => c.name || c.Name).join(', ')}${columns.length > 10 ? '...' : ''}]`);

    // Build column name list
    const columnNames = columns.map(col => col.name || col.Name);

    // Convert each row array into a flat object using column names as keys
    const records = rows.map(row => {
        const record = {};
        columnNames.forEach((colName, index) => {
            record[colName] = row[index] !== undefined ? row[index] : null;
        });
        return record;
    });

    console.log(`Parsed ${records.length} records from tabular format`);
    if (records.length > 0) {
        console.log(`Sample fields: [${Object.keys(records[0]).slice(0, 8).join(', ')}]`);
    }

    return records;
}

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// JSON Upload Routes
app.use('/api', jsonUploadRouter);

// Initialize BigQuery client
const bigquery = new BigQuery({
    projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
});
// Add these RDBMS endpoints after line 20 in your server.js file

// RDBMS Connection Testing
app.post('/api/test-rdbms-connection', async (req, res) => {
    try {
    const { dbType, ...connectionConfig } = req.body;
    
    // ADD THIS DEBUG LINE
    console.log('ðŸ” TEST CONNECTION REQUEST:', { dbType, ...connectionConfig, password: '***' });
        
        console.log(`Testing ${dbType} connection:`, {
            host: connectionConfig.host || connectionConfig.server,
            port: connectionConfig.port,
            database: connectionConfig.database || connectionConfig.service,
            user: connectionConfig.username
        });

        if (!dbType) {
            return res.status(400).json({
                success: false,
                error: 'Database type is required',
                suggestions: ['Specify dbType as postgresql, mysql, oracle, or sqlserver']
            });
        }

        const result = await RDBMSIntegrationService.testConnection(dbType, connectionConfig);
        
        res.json({
            success: result.success,
            message: result.message,
            details: result.details,
            error: result.error,
            suggestions: result.suggestions
        });

    } catch (error) {
        console.error('RDBMS connection test error:', error);
        res.status(500).json({
            success: false,
            error: 'Connection test failed: ' + error.message,
            suggestions: [
                'Check your connection parameters',
                'Ensure the database server is accessible',
                'Verify network connectivity'
            ]
        });
    }
});

// RDBMS Schema Analysis
app.post('/api/get-rdbms-schema', async (req, res) => {
    try {
        const { dbType, connectionConfig, tableName } = req.body;
        
        if (!dbType || !connectionConfig || !tableName) {
            return res.status(400).json({
                success: false,
                error: 'Missing required parameters: dbType, connectionConfig, and tableName are required'
            });
        }

        console.log(`Getting ${dbType} schema for table: ${tableName}`);

        const result = await RDBMSIntegrationService.getSchemaInfo(dbType, connectionConfig, tableName);
        
        res.json({
            success: true,
            data: result
        });

    } catch (error) {
        console.error('RDBMS schema analysis error:', error);
        res.status(500).json({
            success: false,
            error: 'Schema analysis failed: ' + error.message,
            suggestions: [
                'Verify the table name exists',
                'Check if the user has SELECT privileges on the table',
                'Ensure the database connection is valid'
            ]
        });
    }
});

// ENHANCED: RDBMS vs BigQuery Comparison with comprehensive metrics
app.post('/api/rdbms-vs-bq', async (req, res) => {
    try {
        const { dbType, host, port, database, sid, serviceName, username, password, sourceTable, bqTable, primaryKey, comparisonFields = [], sourceFilter = '' } = req.body;

        // âœ… SAFETY: Ignore comparison fields for multi-table validation
        const sourceTablesArray = Array.isArray(sourceTable) 
            ? sourceTable 
            : (sourceTable.includes('\n') ? sourceTable.split('\n').map(t => t.trim()).filter(t => t) : [sourceTable]);
        
        let safeComparisonFields = comparisonFields;
        if (sourceTablesArray.length > 1 && comparisonFields && comparisonFields.length > 0) {
            console.warn(`âš ï¸ Multi-table validation with ${sourceTablesArray.length} tables - ignoring comparison fields for safety`);
            safeComparisonFields = []; // Force empty for multi-table
        }

        console.log(`Starting ENHANCED ${dbType} vs BigQuery comparison...`);
        console.log('Request parameters:', { dbType, host, port, database, sid, serviceName, sourceTable, bqTable, primaryKey });
        
        // Step 1: Test connection
           const connectionConfig = {
           host,
           port: parseInt(port) || (dbType === 'oracle' ? 1521 : 5432),
           username,
           password,
           database
        };

        // Add Oracle-specific connection identifier
        if (dbType === 'oracle') {
           if (sid) {
           connectionConfig.sid = sid;
           console.log(`Oracle using SID: ${sid}`);
        } else if (serviceName) {
        connectionConfig.serviceName = serviceName;
        console.log(`Oracle using Service Name: ${serviceName}`);
    }
}
        
        const connectionTest = await RDBMSIntegrationService.testConnection(dbType, connectionConfig);
        
        if (!connectionTest.success) {
            return res.status(400).json({ 
                success: false, 
                error: `${dbType.toUpperCase()} connection failed: ${connectionTest.error}`,
                suggestions: connectionTest.suggestions
            });
        }

        // STEP 1: Get total count (fast - no data transfer)
        console.log(`ðŸ“Š Getting total record count from ${sourceTable}...`);
        let totalRecordCount = 0;
        let countQuery;

        switch(dbType.toLowerCase()) {
            case 'oracle':
                countQuery = `SELECT COUNT(*) as total_count FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
                break;
            case 'postgresql':
                countQuery = `SELECT COUNT(*) as total_count FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
                break;
            case 'mysql':
                countQuery = `SELECT COUNT(*) as total_count FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
                break;
            case 'sqlserver':
                countQuery = `SELECT COUNT(*) as total_count FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
                break;
            default:
                countQuery = `SELECT COUNT(*) as total_count FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
        }

        try {
            let countResult;
            if (dbType.toLowerCase() === 'oracle') {
                countResult = await RDBMSIntegrationService.fetchOracleData(connectionConfig, countQuery);
            } else {
                countResult = await RDBMSIntegrationService.fetchData(dbType, connectionConfig, countQuery);
            }
            totalRecordCount = parseInt(countResult.records[0]?.total_count || countResult.records[0]?.TOTAL_COUNT || 0, 10);
            console.log(`âœ… Total records: ${totalRecordCount.toLocaleString()}`);
        } catch (countError) {
            console.warn('Count query failed:', countError.message);
        }

        // STEP 2: Fetch sample data (2000 records)
        const SAMPLE_SIZE = 2000;
        console.log(`ðŸ“¦ Fetching ${SAMPLE_SIZE} sample records for validation...`);

        const fields = ['*'];
        let query;

        switch(dbType.toLowerCase()) {
            case 'oracle':
                query = `SELECT ${fields.join(', ')} FROM ${sourceTable} WHERE ${sourceFilter ? `${sourceFilter} AND ` : ''}ROWNUM <= ${SAMPLE_SIZE}`;
                break;
            case 'postgresql':
                query = `SELECT ${fields.join(', ')} FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''} LIMIT ${SAMPLE_SIZE}`;
                break;
            case 'mysql':
                query = `SELECT ${fields.join(', ')} FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''} LIMIT ${SAMPLE_SIZE}`;
                break;
            case 'sqlserver':
                query = `SELECT TOP ${SAMPLE_SIZE} ${fields.join(', ')} FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''}`;
                break;
            default:
                query = `SELECT ${fields.join(', ')} FROM ${sourceTable}${sourceFilter ? ` WHERE ${sourceFilter}` : ''} LIMIT ${SAMPLE_SIZE}`;
        }

        console.log(`Executing query: ${query}`);
        
        // Fetch data
        let rdbmsResult;
        if (dbType.toLowerCase() === 'oracle') {
            rdbmsResult = await RDBMSIntegrationService.fetchOracleData(connectionConfig, query);
        } else {
            rdbmsResult = await RDBMSIntegrationService.fetchData(dbType, connectionConfig, query);
        }
        
        console.log(`âœ… Retrieved ${rdbmsResult.recordCount} sample records from ${dbType.toUpperCase()}`);
        
        if (!rdbmsResult.records || rdbmsResult.records.length === 0) {
            return res.json({
                success: false,
                error: `No data found in source table ${sourceTable}`,
                suggestions: ['Check if the table exists and has data', 'Verify table permissions']
            });
        }

        // Step 2: Create temp BigQuery table from RDBMS data
        console.log('=== RDBMS DATA DEBUG ===');
        console.log('RDBMS data sample:', JSON.stringify(rdbmsResult.records[0], null, 2));
        console.log('Records count:', rdbmsResult.records.length);
        console.log('Primary key:', primaryKey);
        console.log('=======================');
        
        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(
            rdbmsResult.records, 
            `${dbType}_${Date.now()}`,
            primaryKey
        );
        
        console.log(`Created temp table: ${tempTableResult.tempTableId}`);

        // Step 3: Use ENHANCED RDBMS comparison engine
        console.log('ðŸ” Using ENHANCED RDBMS Comparison Engine with comprehensive metrics...');
        const rdbmsComparisonEngine = new RDBMSComparisonEngineService();
        
       const results = await rdbmsComparisonEngine.compareJSONvsBigQuery(
    tempTableResult.tempTableId,
    bqTable,
    primaryKey,
    safeComparisonFields,
    'enhanced',
    totalRecordCount,
    sourceFilter ? true : false
);

        // Add enhanced metadata
        results.metadata = {
            ...results.metadata,
            sourceType: dbType.toUpperCase(),
            sourceTable: sourceTable,
            targetTable: bqTable,
            tempTable: tempTableResult.tempTableId,
            recordsProcessed: rdbmsResult.recordCount,
            totalRecordsInSource: totalRecordCount,
            sampleRecordsValidated: rdbmsResult.recordCount,
            validationApproach: 'sample-based-enhanced',
            sourceFilter: sourceFilter || 'None',
            samplingNote: totalRecordCount > 0 
                ? `Total: ${totalRecordCount.toLocaleString()} records. Validated ${rdbmsResult.recordCount} sample with comprehensive metrics.`
                : `Validated ${rdbmsResult.recordCount} sample records with comprehensive metrics.`,
            enhancedMetrics: true,
            metricsIncluded: [
                'Identical Records',
                'Mismatched Records (same PK, different data)',
                'Missing in Target',
                'NULL Primary Keys (Source & Target)'
            ]
        };

        // Update summary with enhanced metrics
        results.summary = {
            ...results.summary,
            totalRecordsInSource: totalRecordCount > 0 ? totalRecordCount : rdbmsResult.recordCount,
            sampleRecordsValidated: rdbmsResult.recordCount,
            isSampleBased: true,
            enhancedValidation: true
        };

        console.log(`${dbType.toUpperCase()} vs BigQuery ENHANCED comparison completed`);
        console.log(`ðŸ“Š Results: ${results.summary.identicalRecords || 0} identical, ${results.summary.mismatchedRecords || 0} mismatched`);
        
        res.json(results);

    } catch (error) {
        console.error('RDBMS vs BigQuery comparison failed:', error.message);
        
        let suggestions = [
            'Check database connection parameters',
            'Verify source table exists and has data',
            'Ensure BigQuery table is accessible'
        ];

        if (error.message.includes('ENOTFOUND')) {
            suggestions.unshift('Network connectivity issue - check VPN or network access');
        }

        res.status(500).json({ 
            success: false, 
            error: error.message,
            suggestions: suggestions
        });
    }
});


// UTILITY: Consistent JSON parsing function used across all endpoints
function parseJsonContent(fileContent, fileName = 'unknown') {
    let jsonData = [];
    let detectedFormat = 'Unknown';
    let parseMethod = 'None';
    
    console.log(`Parsing JSON content for: ${fileName}`);
    console.log(`Content preview: ${fileContent.substring(0, 100)}...`);
    
    // Strategy 1: Try standard JSON parsing first (handles arrays and objects)
    try {
        console.log('Strategy 1: Attempting standard JSON.parse()...');
        const parsed = JSON.parse(fileContent);
        
        if (Array.isArray(parsed)) {
            jsonData = parsed;
            detectedFormat = 'JSON Array';
            parseMethod = 'JSON.parse() - Array';
            console.log(`SUCCESS: Parsed as JSON Array with ${jsonData.length} records`);
        } else if (typeof parsed === 'object' && parsed !== null) {
            jsonData = [parsed];
            detectedFormat = 'JSON Object';
            parseMethod = 'JSON.parse() - Single Object';
            console.log(`SUCCESS: Parsed as single JSON Object`);
        } else {
            throw new Error('Parsed content is not an object or array');
        }
        
    } catch (standardJsonError) {
        console.log(`Strategy 1 FAILED: ${standardJsonError.message}`);
        
        // Strategy 2: Try JSONL parsing (line-delimited JSON)
        try {
            console.log('Strategy 2: Attempting JSONL parsing...');
            const lines = fileContent.trim().split('\n');
            console.log(`Found ${lines.length} lines to process`);
            
            let validLines = 0;
            for (const line of lines) {
                const trimmedLine = line.trim();
                if (trimmedLine) {
                    try {
                        const record = JSON.parse(trimmedLine);
                        jsonData.push(record);
                        validLines++;
                    } catch (lineError) {
                        console.warn(`Skipping invalid JSON line: ${lineError.message}`);
                    }
                }
            }
            
            if (jsonData.length > 0) {
                detectedFormat = 'JSONL';
                parseMethod = 'Line-by-line parsing';
                console.log(`SUCCESS: Parsed as JSONL with ${jsonData.length} valid records from ${validLines} lines`);
            } else {
                throw new Error('No valid JSON records found in JSONL format');
            }
            
        } catch (jsonlError) {
            console.log(`Strategy 2 FAILED: ${jsonlError.message}`);
            throw new Error(`All parsing strategies failed. JSON error: ${standardJsonError.message}. JSONL error: ${jsonlError.message}`);
        }
    }
    
    return {
        jsonData: jsonData,
        detectedFormat: detectedFormat,
        parseMethod: parseMethod
    };
}

// FIXED: Schema Analysis Endpoint - Now accepts custom source table
app.post('/api/analyze-schemas', async (req, res) => {
    try {
        const { tempTableId, sourceTable } = req.body;
        
        if (!tempTableId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId and sourceTable are required'
            });
        }
        
        console.log('Analyzing schemas for comprehensive comparison...');
        console.log(`Using USER-SPECIFIED source table: ${sourceTable}`);
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const schemaAnalysis = await comparisonEngine.getCommonFields(tempTableId, sourceTable);
        
        console.log(`Schema analysis complete: ${schemaAnalysis.commonFields.length} common fields found`);
        
        res.json({
            success: true,
            ...schemaAnalysis
        });
        
    } catch (error) {
        console.error('Schema analysis failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Schema analysis failed'
        });
    }
});

// FIXED: Create Temp Table - Now with CONSISTENT JSON parsing + handles large files
app.post('/api/create-temp-table', async (req, res) => {
    try {
        const { fileId, primaryKey } = req.body;
        
        if (!fileId) {
            return res.status(400).json({
                success: false,
                error: 'File ID is required'
            });
        }
        
        console.log(`Creating temp table for file: ${fileId}`);
        console.log(`Using primary key for verification: ${primaryKey || 'none specified'}`);
        
        const fs = require('fs');
        const path = require('path');
        
        const possiblePaths = [
            path.join(__dirname, 'uploads', `${fileId}.json`),
            path.join(__dirname, 'uploads', `${fileId}.jsonl`),
            path.join(__dirname, 'uploads', fileId),
            path.join(__dirname, 'temp-files', `${fileId}.json`),
            path.join(__dirname, 'temp-files', `${fileId}.jsonl`)
        ];
        
        let filePath = null;
        for (const possiblePath of possiblePaths) {
            if (fs.existsSync(possiblePath)) {
                filePath = possiblePath;
                console.log(`Found file at: ${filePath}`);
                break;
            }
        }
        
        if (!filePath) {
            console.log('File not found in any expected location');
            return res.status(404).json({
                success: false,
                error: 'File not found',
                details: `File ${fileId} not found`
            });
        }
        
        console.log(`Reading file: ${filePath}`);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        
        // CONSISTENT: Use the same parsing logic as preview
        let parseResult;
        try {
            parseResult = parseJsonContent(fileContent, path.basename(filePath));
        } catch (parseError) {
            console.error('JSON parsing failed:', parseError.message);
            return res.status(400).json({
                success: false,
                error: 'Invalid JSON format',
                details: parseError.message
            });
        }
        
        const jsonData = parseResult.jsonData;
        console.log(`Parsed ${jsonData.length} records using ${parseResult.parseMethod}`);
        
        if (jsonData.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No valid JSON data found in file'
            });
        }
        
        // Log available fields for debugging
        console.log('Available fields in JSON data:', Object.keys(jsonData[0] || {}));
        
        // Flatten nested objects for BigQuery compatibility
        const flattenedData = jsonData.map((record) => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        // Handle nested objects (ServiceNow references, AWS structures, etc.)
                        if (value.display_value || value.link || value.value) {
                            if (value.display_value) {
                                flattened[`${newKey}_display_value`] = String(value.display_value);
                            }
                            if (value.link) {
                                flattened[`${newKey}_link`] = String(value.link);
                            }
                            if (value.value) {
                                flattened[`${newKey}_value`] = String(value.value);
                            }
                        } else {
                            // Limit nesting depth to prevent overly complex structures
                            if (prefix.split('_').length < 3) {
                                flattenObject(value, newKey);
                            } else {
                                flattened[newKey] = JSON.stringify(value);
                            }
                        }
                    } else if (Array.isArray(value)) {
                        flattened[newKey] = JSON.stringify(value);
                    } else {
                        if (value === null || value === undefined) {
                            flattened[newKey] = null;
                        } else {
                            flattened[newKey] = String(value);
                        }
                    }
                }
            }
            
            flattenObject(record);
            return flattened;
        });
        
        console.log(`Flattened data ready for BigQuery`);
        
        // Create temp table with dynamic primary key for verification + batch processing
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(flattenedData, fileId, primaryKey);
        
        console.log('Temp table creation completed');
        console.log(`Records in table: ${result.recordsInTable}`);
        console.log(`Actual temp table ID: ${result.tempTableId}`);
        
        res.json({
            success: true,
            message: result.message,
            tempTableId: result.tempTableId,
            tempTableName: result.tempTableName,
            recordsUploaded: result.recordsInTable,
            recordsAttempted: result.inputRecords,
            recordCountMatch: result.recordCountMatch,
            fieldsProcessed: result.fieldsProcessed,
            approach: result.approach,
            batchInfo: result.batchInfo,
            verification: result.verification,
            expiresAt: result.expiresAt,
            parseInfo: {
                format: parseResult.detectedFormat,
                method: parseResult.parseMethod
            },
            fixes: result.fixes || ['Universal data type support', 'Dynamic primary key support', 'Batch processing for large files']
        });
        
    } catch (error) {
        console.error('Temp table creation failed:', error.message);
        
        let errorMessage = error.message;
        let suggestions = [
            'Check your file format and structure',
            'Verify you have proper BigQuery permissions',
            'Try with a smaller file first to test functionality'
        ];
        
        if (error.message.includes('Request Entity Too Large') || error.message.includes('413')) {
            suggestions = [
                'File is too large for single batch processing',
                'System will automatically use batch processing for large files',
                'Try uploading the file again - batch processing should handle it',
                'If issue persists, try breaking the file into smaller chunks'
            ];
        }
        
        res.status(500).json({
            success: false,
            error: errorMessage,
            details: 'Failed to create temp table from JSON',
            suggestions: suggestions
        });
    }
});


// ENHANCED: ROBUST JSON File Preview Endpoint - CONSISTENT parsing with create-temp-table
app.get('/api/preview-json/:fileId', async (req, res) => {
    try {
        const { fileId } = req.params;
        console.log(`=== STARTING PREVIEW FOR FILE: ${fileId} ===`);
        
        const fs = require('fs');
        const path = require('path');
        
        // Check multiple possible file locations
        const possiblePaths = [
            path.join(__dirname, 'uploads', `${fileId}.json`),
            path.join(__dirname, 'uploads', `${fileId}.jsonl`),
            path.join(__dirname, 'uploads', fileId),
            path.join(__dirname, 'temp-files', `${fileId}.json`),
            path.join(__dirname, 'temp-files', `${fileId}.jsonl`)
        ];
        
        let filePath = null;
        for (const possiblePath of possiblePaths) {
            if (fs.existsSync(possiblePath)) {
                filePath = possiblePath;
                console.log(`Found file at: ${filePath}`);
                break;
            }
        }
        
        if (!filePath) {
            console.log('File not found for preview in any expected location');
            return res.status(404).json({
                success: false,
                error: 'File not found for preview',
                details: `File ${fileId} not found in any upload directory`
            });
        }
        
        // Read file content and stats
        console.log(`Reading file content from: ${filePath}`);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const fileStat = fs.statSync(filePath);
        
        console.log(`File size: ${fileStat.size} bytes`);
        
        // CONSISTENT: Use the same parsing logic as create-temp-table
        let parseResult;
        try {
            parseResult = parseJsonContent(fileContent, path.basename(filePath));
        } catch (parseError) {
            console.error('Preview JSON parsing failed:', parseError.message);
            return res.status(400).json({
                success: false,
                error: 'Invalid JSON format for preview',
                details: parseError.message,
                suggestions: [
                    'Verify JSON file is properly formatted',
                    'Check for missing commas or brackets',
                    'Ensure file is either valid JSON array or JSONL format',
                    'Try validating JSON in an online JSON validator'
                ]
            });
        }
        
        const jsonData = parseResult.jsonData;
        console.log(`Preview parsing successful: ${jsonData.length} records using ${parseResult.parseMethod}`);
        
        if (jsonData.length === 0) {
            console.error('No JSON data was successfully parsed for preview');
            return res.status(400).json({
                success: false,
                error: 'No valid JSON data found in file',
                details: 'File was readable but contained no valid JSON data'
            });
        }
        
        // Generate comprehensive field analysis
        console.log('=== GENERATING FIELD ANALYSIS ===');
        
        // Flatten the first record to understand the full field structure
        const firstRecord = jsonData[0];
        const flattenedSample = {};
        
        function flattenObject(obj, prefix = '', depth = 0) {
            // Prevent infinite recursion
            if (depth > 5) {
                console.warn(`Max flattening depth reached for prefix: ${prefix}`);
                return;
            }
            
            for (const [key, value] of Object.entries(obj)) {
                const cleanKey = prefix ? `${prefix}_${key}` : key;
                
                if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                    // Handle ServiceNow-style objects with display_value/link/value
                    if (value.display_value !== undefined || value.link !== undefined || value.value !== undefined) {
                        if (value.display_value !== undefined) {
                            flattenedSample[`${cleanKey}_display_value`] = value.display_value;
                        }
                        if (value.link !== undefined) {
                            flattenedSample[`${cleanKey}_link`] = value.link;
                        }
                        if (value.value !== undefined) {
                            flattenedSample[`${cleanKey}_value`] = value.value;
                        }
                    } else {
                        // Regular nested object - flatten recursively
                        flattenObject(value, cleanKey, depth + 1);
                    }
                } else if (Array.isArray(value)) {
                    // Convert arrays to JSON strings
                    flattenedSample[cleanKey] = JSON.stringify(value);
                } else {
                    // Simple value
                    flattenedSample[cleanKey] = value;
                }
            }
        }
        
        try {
            flattenObject(firstRecord);
            console.log(`Flattening completed: ${Object.keys(flattenedSample).length} fields generated`);
        } catch (flattenError) {
            console.error(`Flattening failed: ${flattenError.message}`);
            // Fallback to original fields
            Object.assign(flattenedSample, firstRecord);
        }
        
        // Get all available fields
        const allFields = Object.keys(flattenedSample);
        console.log(`Total fields available: ${allFields.length}`);
        console.log(`Sample fields: [${allFields.slice(0, 10).join(', ')}]`);
        
        // Smart field categorization
        const idFields = allFields.filter(field => {
            const lowerField = field.toLowerCase();
            return lowerField.includes('id') || 
                   lowerField.includes('key') || 
                   lowerField.includes('number') ||
                   lowerField === 'arn' ||
                   lowerField === 'catalog' ||
                   lowerField.endsWith('_id') ||
                   lowerField.startsWith('id_') ||
                   lowerField.includes('identifier');
        });
        
        const importantFields = allFields.filter(field => {
            const lowerField = field.toLowerCase();
            return !idFields.includes(field) && (
                lowerField.includes('name') ||
                lowerField.includes('account') ||
                lowerField.includes('type') ||
                lowerField.includes('status') ||
                lowerField.includes('code') ||
                lowerField.includes('date') ||
                lowerField.includes('stage') ||
                lowerField.includes('category') ||
                lowerField.includes('healthy') ||
                lowerField.includes('enabled') ||
                lowerField.includes('monitor')
            );
        });
        
        console.log(`Field categorization complete:`);
        console.log(`  ID/Key fields: ${idFields.length} [${idFields.slice(0, 5).join(', ')}]`);
        console.log(`  Important fields: ${importantFields.length} [${importantFields.slice(0, 5).join(', ')}]`);
        
        // Create comprehensive preview response
        const preview = {
            totalRecords: jsonData.length,
            fieldsDetected: allFields.length,
            fileSize: fileStat.size,
            format: parseResult.detectedFormat,
            parseMethod: parseResult.parseMethod,
            sampleRecords: [flattenedSample], // Send flattened version for field suggestions
            availableFields: allFields,
            idFields: idFields,
            importantFields: importantFields,
            allFieldsList: allFields.slice(0, 100),
            originalSample: jsonData[0], // Also send original for reference
            processingDetails: {
                detectedFormat: parseResult.detectedFormat,
                parseMethod: parseResult.parseMethod,
                flatteningSuccess: Object.keys(flattenedSample).length > Object.keys(firstRecord).length,
                timestamp: new Date().toISOString()
            }
        };
        
        console.log(`=== PREVIEW GENERATION COMPLETE ===`);
        console.log(`Preview created successfully:`);
        console.log(`  - Records: ${preview.totalRecords}`);
        console.log(`  - Fields: ${preview.fieldsDetected}`);
        console.log(`  - Format: ${preview.format}`);
        console.log(`  - ID Fields: ${preview.idFields.length}`);
        console.log(`  - Important Fields: ${preview.importantFields.length}`);
        
        res.json({
            success: true,
            preview: preview
        });
        
    } catch (error) {
        console.error('=== PREVIEW GENERATION FAILED ===');
        console.error(`Error: ${error.message}`);
        console.error(`Stack: ${error.stack}`);
        
        res.status(500).json({
            success: false,
            error: 'Preview generation failed',
            details: error.message,
            suggestions: [
                'Check that the uploaded file is valid JSON',
                'Verify file is not corrupted',
                'Try uploading a smaller test file first',
                'Contact support if issue persists'
            ]
        });
    }
});


// ENHANCED: JSON vs BigQuery Comparison - Now with UNIVERSAL DATA TYPES + DUAL DUPLICATES ANALYSIS
app.post('/api/compare-json-vs-bq', async (req, res) => {
    try {
        const { 
            fileId, 
            sourceTable,  // USER-SPECIFIED BigQuery table
            primaryKey,   // USER-SPECIFIED primary key (ANY DATA TYPE)
            comparisonFields = [],
            strategy = 'enhanced' 
        } = req.body;
        
        console.log(`ENHANCED: Starting UNIVERSAL DATA TYPE comparison for file: ${fileId}`);
        console.log(`User-specified BigQuery table: ${sourceTable}`);
        console.log(`User-specified primary key: ${primaryKey} (supports ANY data type)`);
        
        if (!fileId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'fileId and sourceTable are required'
            });
        }

        if (!primaryKey || primaryKey.trim() === '') {
            return res.status(400).json({
                success: false,
                error: 'Primary key field is required',
                suggestions: [
                    'Enter ANY field name that exists in both JSON and BigQuery tables',
                    'Supports ALL data types: STRING, INT64, FLOAT64, BOOLEAN, DATE, DATETIME, TIMESTAMP, NUMERIC, etc.',
                    'For monitor data, try: id, account_id (numeric or string)',
                    'For AWS data, try: Id, Arn, Catalog (any type)',
                    'For ServiceNow data, try: task_sys_id, task_number (any type)',
                    'For date/timestamp keys: created_date, updated_at (date/timestamp types)',
                    'System automatically handles data type conversion for comparison'
                ]
            });
        }
        
        // Find and parse the JSON file
        const fs = require('fs');
        const path = require('path');
        
        const possiblePaths = [
            path.join(__dirname, 'uploads', `${fileId}.json`),
            path.join(__dirname, 'uploads', `${fileId}.jsonl`),
            path.join(__dirname, 'uploads', fileId),
            path.join(__dirname, 'temp-files', `${fileId}.json`),
            path.join(__dirname, 'temp-files', `${fileId}.jsonl`)
        ];
        
        let filePath = null;
        for (const possiblePath of possiblePaths) {
            if (fs.existsSync(possiblePath)) {
                filePath = possiblePath;
                break;
            }
        }
        
        if (!filePath) {
            return res.status(404).json({
                success: false,
                error: 'File not found for comparison'
            });
        }
        
        // Parse the JSON data with CONSISTENT parsing logic
        const fileContent = fs.readFileSync(filePath, 'utf8');
        
        let parseResult;
        try {
            parseResult = parseJsonContent(fileContent, path.basename(filePath));
        } catch (parseError) {
            return res.status(400).json({
                success: false,
                error: 'Invalid JSON format for comparison',
                details: parseError.message
            });
        }
        
        const jsonData = parseResult.jsonData;
        console.log(`Re-parsed ${jsonData.length} records for comparison using ${parseResult.parseMethod}`);
        
        // Flatten the data (same as create-temp-table)
        const flattenedData = jsonData.map((record) => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        if (value.display_value || value.link || value.value) {
                            if (value.display_value) {
                                flattened[`${newKey}_display_value`] = String(value.display_value);
                            }
                            if (value.link) {
                                flattened[`${newKey}_link`] = String(value.link);
                            }
                            if (value.value) {
                                flattened[`${newKey}_value`] = String(value.value);
                            }
                        } else {
                            if (prefix.split('_').length < 3) {
                                flattenObject(value, newKey);
                            } else {
                                flattened[newKey] = JSON.stringify(value);
                            }
                        }
                    } else if (Array.isArray(value)) {
                        flattened[newKey] = JSON.stringify(value);
                    } else {
                        if (value === null || value === undefined) {
                            flattened[newKey] = null;
                        } else {
                            flattened[newKey] = String(value);
                        }
                    }
                }
            }
            
            flattenObject(record);
            return flattened;
        });
        
        // Create temp table with user's primary key for verification + batch processing
        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(flattenedData, fileId, primaryKey);
        
        console.log(`Temp table created successfully`);
        console.log(`ACTUAL temp table ID: ${tempTableResult.tempTableId}`);
        
        // Use the actual temp table ID returned from creation
        const actualTempTableId = tempTableResult.tempTableId;
        
        console.log(`ENHANCED: Comparing using actual table: ${actualTempTableId} vs ${sourceTable}`);
        console.log(`Using UNIVERSAL DATA TYPE support for primary key: ${primaryKey}`);
        
        // Pre-comparison verification
        try {
            const [preCheckResult] = await bigquery.query(`SELECT COUNT(*) as count FROM \`${actualTempTableId}\``);
            const tempTableCount = preCheckResult[0].count;
            console.log(`Pre-comparison check: ${tempTableCount} records in temp table`);
            
            if (tempTableCount === 0) {
                console.error(`CRITICAL: Temp table is empty!`);
                return res.status(400).json({
                    success: false,
                    error: 'Temp table is empty',
                    details: `No records found in temp table: ${actualTempTableId}`
                });
            }
        } catch (preCheckError) {
            console.error(`Pre-comparison check failed:`, preCheckError.message);
            return res.status(400).json({
                success: false,
                error: 'Cannot access temp table',
                details: preCheckError.message
            });
        }
        
        // Run ENHANCED comparison with UNIVERSAL DATA TYPE SUPPORT + DUAL DUPLICATES ANALYSIS
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        console.log(`ENHANCED: Running UNIVERSAL data type comparison...`);
        console.log(`Using user's BigQuery table: ${sourceTable}`);
        console.log(`Using user's primary key with universal casting: ${primaryKey}`);
        console.log(`Supports: STRING, INT64, FLOAT64, BOOLEAN, DATE, DATETIME, TIMESTAMP, NUMERIC, TIME, GEOGRAPHY, JSON`);
        
        const results = await comparisonEngine.compareJSONvsBigQuery(
            actualTempTableId, // Use actual table ID
            sourceTable,      // Use user-specified table
            primaryKey,       // Use user-specified primary key (ANY DATA TYPE)
            comparisonFields,
            strategy
        );
        
        console.log(`ENHANCED comparison completed successfully`);
        console.log(`Results summary: ${results.summary?.recordsReachedTarget || 0} matches found using '${primaryKey}' with universal data type support`);
        console.log(`Data types detected: JSON ${results.comparisonResults?.dataTypes?.tempType || 'STRING'} â†” BQ ${results.comparisonResults?.dataTypes?.sourceType || 'STRING'}`);
        console.log(`Duplicates analysis: JSON has ${results.duplicatesAnalysis?.jsonDuplicates?.duplicateCount || 0}, BQ has ${results.duplicatesAnalysis?.bqDuplicates?.duplicateCount || 0} duplicate keys`);
        
        // Include enhanced temp table info in response
        results.tempTableInfo = {
            actualTableId: actualTempTableId,
            recordsInTable: tempTableResult.recordsInTable,
            recordCountMatch: tempTableResult.recordCountMatch,
            batchInfo: tempTableResult.batchInfo,
            parseInfo: {
                format: parseResult.detectedFormat,
                method: parseResult.parseMethod
            }
        };
        
        // Add enhanced capabilities info
        results.enhancedCapabilities = {
            universalDataTypeSupport: true,
            supportedTypes: ['STRING', 'INT64', 'FLOAT64', 'BOOLEAN', 'DATE', 'DATETIME', 'TIMESTAMP', 'NUMERIC', 'TIME', 'GEOGRAPHY', 'JSON'],
            dualSystemDuplicatesAnalysis: true,
            excelExportReady: true,
            dataTypesDetected: results.comparisonResults?.dataTypes || { tempType: 'STRING', sourceType: 'STRING' }
        };
        
        res.json(results);
        
    } catch (error) {
        console.error('Enhanced comparison API failed:', error.message);
        
        // Enhanced error handling for schema and data type issues
        let errorMessage = error.message;
        let suggestions = [
            'Check that the primary key field exists in both JSON and BigQuery tables',
            'System supports ALL data types - the issue may be field name mismatch',
            'Try using a different field that exists in both tables',
            'Verify BigQuery table is accessible'
        ];
        
        if (error.message.includes('not available in both tables')) {
            suggestions = [
                'Choose a field that exists in both your JSON file and BigQuery table',
                'SUPPORTS ANY DATA TYPE: numeric, string, boolean, date, timestamp, etc.',
                'For monitor data, try: id, account_id (any numeric or string type)',
                'For AWS data, try: Id, Arn, Catalog (any data type)',
                'For ServiceNow data, try: task_sys_id, task_number (any data type)',
                'For date/time data, try: created_date, updated_at (date/timestamp types)',
                'Check the Column Names tab after upload to see available common fields'
            ];
        } else if (error.message.includes('Unrecognized name')) {
            suggestions = [
                'The selected field does not exist in one of the tables',
                'Use the Column Names tab to see which fields are available in both tables',
                'Try a different primary key field',
                'Field names are case-sensitive - ensure exact match',
                'System handles data type conversion automatically'
            ];
        } else if (error.message.includes('No matching signature')) {
            suggestions = [
                'FIXED: This data type comparison error has been resolved',
                'System now supports ALL BigQuery data types with automatic casting',
                'Try the comparison again - universal data type support is now active',
                'If the issue persists, the field may not exist in one of the tables'
            ];
        } else if (error.message.includes('Request Entity Too Large') || error.message.includes('413')) {
            suggestions = [
                'File was processed with batch processing for large files',
                'System automatically handles large files up to 100MB',
                'Batch processing was successful, comparison should work normally'
            ];
        }
        
        res.status(500).json({
            success: false,
            error: errorMessage,
            details: 'Enhanced comparison with universal data type support failed',
            suggestions: suggestions,
            capabilities: {
                universalDataTypeSupport: true,
                supportedTypes: 'ALL BigQuery types (STRING, INT64, FLOAT64, BOOLEAN, DATE, DATETIME, TIMESTAMP, NUMERIC, etc.)',
                dualDuplicatesAnalysis: true
            },
            timestamp: new Date().toISOString()
        });
    }
});


// BigQuery Connection Test Endpoint
app.get('/api/test-bq-connection', async (req, res) => {
    try {
        console.log('Testing BigQuery connection via API...');
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.testConnection();
        res.json(result);
    } catch (error) {
        console.error('BigQuery connection test failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'BigQuery connection test failed'
        });
    }
});

// Test Source Table Access - Now accepts custom table
app.get('/api/test-source-table', async (req, res) => {
    try {
        const { sourceTable } = req.query;
        console.log('Testing source table access via API...');
        console.log(`Testing table: ${sourceTable || 'default'}`);
        
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.testSourceTableAccess(sourceTable);
        res.json(result);
    } catch (error) {
        console.error('Source table test failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Source table access test failed'
        });
    }
});

// Manual Cleanup Endpoint
app.delete('/api/cleanup-temp-table/:fileId', async (req, res) => {
    try {
        const { fileId } = req.params;
        console.log(`Manual cleanup for file: ${fileId}`);
        
        const dataset = bigquery.dataset('temp_validation_tables');
        
        let deletedTables = [];
        let errors = [];
        
        try {
            const [tables] = await dataset.getTables();
            const relevantTables = tables.filter(table => 
                table.id.startsWith(`json_temp_${fileId}`)
            );
            
            console.log(`Found ${relevantTables.length} relevant tables to clean up`);
            
            for (const table of relevantTables) {
                try {
                    await table.delete();
                    deletedTables.push(table.id);
                    console.log(`Deleted table: ${table.id}`);
                } catch (deleteError) {
                    errors.push(`Failed to delete ${table.id}: ${deleteError.message}`);
                    console.error(`Failed to delete ${table.id}:`, deleteError.message);
                }
            }
            
        } catch (listError) {
            console.error(`Failed to list tables:`, listError.message);
            errors.push(`Failed to list tables: ${listError.message}`);
        }
        
        res.json({
            success: deletedTables.length > 0 || errors.length === 0,
            deletedTables: deletedTables,
            errors: errors,
            message: `Cleanup completed: ${deletedTables.length} tables deleted, ${errors.length} errors`
        });
        
    } catch (error) {
        console.error('Manual cleanup failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ENHANCED: Sanity Test (formerly Table Validation) - v1.0 functionality preserved
app.post('/api/validate', async (req, res) => {
    try {
        const {
            tableName,
            nullCheckColumns,
            duplicateKeyColumns,
            specialCharCheckColumns,
            compositeKeyColumns
        } = req.body;

        if (!tableName) {
            return res.status(400).json({
                success: false,
                error: {
                    type: 'MISSING_TABLE_NAME',
                    title: 'Missing Table Name',
                    message: 'Table name is required to run sanity test.',
                    suggestions: [
                        'Enter a valid BigQuery table name',
                        'Use format: project.dataset.table'
                    ]
                }
            });
        }

        if (!tableName.includes('.') || tableName.split('.').length !== 3) {
            return res.status(400).json({
                success: false,
                error: {
                    type: 'INVALID_TABLE_FORMAT',
                    title: 'Invalid Table Name Format',
                    message: 'Table name must be in the format: project.dataset.table',
                    details: `Provided table name: ${tableName}`,
                    suggestions: [
                        'Use format: project_id.dataset_name.table_name',
                        'Only use letters, numbers, hyphens, and underscores',
                        'Example: my-project.my_dataset.customer_table'
                    ]
                }
            });
        }

        console.log('Running sanity test for table:', tableName);

        const query = `
            CALL \`${process.env.VALIDATION_PROCEDURE_PROJECT || 'rax-staging-qa'}.${process.env.VALIDATION_PROCEDURE_DATASET || 'stage_three_dw'}.data_validation_checks\`(
                @table_name,
                @null_check_columns,
                @duplicate_key_columns,
                @special_char_check_columns
            )
        `;

        const options = {
            query: query,
            params: {
                table_name: tableName,
                null_check_columns: nullCheckColumns || [],
                duplicate_key_columns: duplicateKeyColumns || [],
                special_char_check_columns: specialCharCheckColumns || []
            },
            types: {
                table_name: 'STRING',
                null_check_columns: ['STRING'],
                duplicate_key_columns: ['STRING'],
                special_char_check_columns: ['STRING']
            }
        };

        const [rows] = await bigquery.query(options);
        console.log('Sanity test results:', rows);

        res.json({
            success: true,
            data: rows,
            timestamp: new Date().toISOString(),
            table: tableName,
            testType: 'sanity-test'
        });

    } catch (error) {
        console.error('Error running sanity test:', error);

        let errorResponse = {
            success: false,
            error: {
                type: 'UNKNOWN_ERROR',
                title: 'Sanity Test Error',
                message: 'An unexpected error occurred',
                details: error.message,
                suggestions: [
                    'Check your table and column names',
                    'Verify you have proper permissions',
                    'Ensure table format is correct'
                ]
            }
        };

        if (error.message.includes('Table') && error.message.includes('not found')) {
            errorResponse.error = {
                type: 'TABLE_NOT_FOUND',
                title: 'Table Not Found',
                message: 'The specified BigQuery table does not exist or is not accessible.',
                details: error.message,
                suggestions: [
                    'Check the table name format: project.dataset.table',
                    'Verify the table exists in BigQuery console',
                    'Ensure you have proper permissions to access the table'
                ]
            };
        } else if (error.message.includes('Column') && error.message.includes('not found')) {
            errorResponse.error = {
                type: 'COLUMN_NOT_FOUND',
                title: 'Column Not Found',
                message: 'One or more specified columns do not exist in the table.',
                details: error.message,
                suggestions: [
                    'Check column names for typos',
                    'Verify column names match exactly (case-sensitive)',
                    'Use BigQuery console to view table schema'
                ]
            };
        } else if (error.message.includes('permission') || 
                   error.message.includes('Permission') || 
                   error.message.includes('Access Denied')) {
            errorResponse.error = {
                type: 'PERMISSION_DENIED',
                title: 'Access Denied',
                message: 'You do not have sufficient permissions to access this resource.',
                details: error.message,
                suggestions: [
                    'Contact your administrator for BigQuery access',
                    'Verify your service account has proper roles'
                ]
            };
        }

        res.status(500).json(errorResponse);
    }
});


// ============================================================
// BQ vs BQ COMPARISON ENDPOINT - FROM QE_Consol_QA
// ============================================================

app.post('/api/bq-vs-bq', async (req, res) => {
    try {
        const { sourceTable, targetTable, primaryKey, comparisonFields = [], sourceFilter = '' } = req.body;
        const SAMPLE_SIZE = 2000;

        console.log(`\n=== Starting BigQuery vs BigQuery Comparison ===`);
        console.log(`Source: ${sourceTable}`);
        console.log(`Target: ${targetTable}`);
        console.log(`Primary Key: ${primaryKey}`);
        console.log(`Source Filter: ${sourceFilter || 'None'}`);
        console.log(`Sample Size: ${SAMPLE_SIZE}`);

        if (!sourceTable || !targetTable || !primaryKey) {
            return res.status(400).json({
                success: false,
                error: 'sourceTable, targetTable, and primaryKey are required'
            });
        }

        const tableRegex = /^[\w-]+\.[\w-]+\.[\w-]+$/;
        if (!sourceTable.match(tableRegex)) {
            return res.status(400).json({ success: false, error: `Invalid source table format: ${sourceTable}. Must be project.dataset.table` });
        }
        if (!targetTable.match(tableRegex)) {
            return res.status(400).json({ success: false, error: `Invalid target table format: ${targetTable}. Must be project.dataset.table` });
        }

        const sourceWhereClause = sourceFilter ? `WHERE ${sourceFilter}` : '';

        // STEP 1: Full record counts
        console.log('Getting full record counts...');
        let sourceTotalCount = 0, targetTotalCount = 0;
        try {
            const countsQuery = sourceFilter ? `
                 SELECT 
                (SELECT COUNT(*) FROM \`${sourceTable}\` ${sourceWhereClause}) as source_total,
                (SELECT COUNT(*) FROM \`${targetTable}\` WHERE SAFE_CAST(${primaryKey} AS STRING) IN (
                SELECT DISTINCT SAFE_CAST(${primaryKey} AS STRING) FROM \`${sourceTable}\` ${sourceWhereClause}
                ${sourceFilter ? 'AND' : 'WHERE'} ${primaryKey} IS NOT NULL
                )) as target_total
                ` : `
            SELECT 
                (SELECT COUNT(*) FROM \`${sourceTable}\`) as source_total,
                (SELECT COUNT(*) FROM \`${targetTable}\`) as target_total
            `;
            const [countRows] = await bigquery.query(countsQuery);
            sourceTotalCount = countRows[0].source_total;
            targetTotalCount = countRows[0].target_total;
            console.log(`Source: ${sourceTotalCount} total, Target: ${targetTotalCount} total`);
        } catch (err) {
            return res.status(400).json({
                success: false,
                error: `Count query failed: ${err.message}`,
                suggestions: ['Verify both tables exist', `Check primary key "${primaryKey}"`, 'Check source filter syntax']
            });
        }

        // STEP 2: Create sample source keys
        console.log('Creating source sample...');
        const sampleSubquery = `
            SELECT DISTINCT SAFE_CAST(${primaryKey} AS STRING) as pk
            FROM \`${sourceTable}\` ${sourceWhereClause}
            ${sourceFilter ? 'AND' : 'WHERE'} ${primaryKey} IS NOT NULL
            LIMIT ${SAMPLE_SIZE}
        `;

        let sampleKeys = [];
        try {
            const [sampleRows] = await bigquery.query(sampleSubquery);
            sampleKeys = sampleRows.map(r => r.pk);
            console.log(`Sample keys: ${sampleKeys.length} selected`);
        } catch (sampleErr) {
            return res.status(400).json({ success: false, error: `Sample query failed: ${sampleErr.message}` });
        }

        if (sampleKeys.length === 0) {
            return res.status(400).json({ success: false, error: 'No records found in source with given filter' });
        }

        const sampleValidated = sampleKeys.length;

        // STEP 3: Sample-based detailed stats
        console.log('Getting sample-based stats...');
        let sampleSourceStats = { unique: sampleValidated, duplicates: 0 };
        let sampleTargetStats = { unique: 0, duplicates: 0 };

        try {
            const sampleStatsQuery = `
                WITH source_sample AS (${sampleSubquery})
                SELECT
                    (SELECT COUNT(DISTINCT pk) FROM source_sample) as source_unique,
                    (SELECT COUNT(*) - COUNT(DISTINCT pk) FROM source_sample) as source_duplicates,
                    (SELECT COUNT(DISTINCT SAFE_CAST(${primaryKey} AS STRING)) FROM \`${targetTable}\` WHERE SAFE_CAST(${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)) as target_unique,
                    (SELECT COUNT(*) - COUNT(DISTINCT SAFE_CAST(${primaryKey} AS STRING)) FROM \`${targetTable}\` WHERE SAFE_CAST(${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)) as target_duplicates
            `;
            const [statsRows] = await bigquery.query(sampleStatsQuery);
            const stats = statsRows[0];
            sampleSourceStats = { unique: stats.source_unique, duplicates: stats.source_duplicates };
            sampleTargetStats = { unique: stats.target_unique, duplicates: stats.target_duplicates };
        } catch (statsErr) {
            console.warn('Sample stats failed:', statsErr.message);
        }

        // STEP 4: Schema analysis
        console.log('Analyzing schema...');
        let sourceFields = [], targetFields = [];
        try {
            const schemaQuery = `
                SELECT col, src FROM (
                    SELECT column_name as col, 'source' as src 
                    FROM \`${sourceTable.split('.')[0]}.${sourceTable.split('.')[1]}\`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '${sourceTable.split('.')[2]}'
                    UNION ALL
                    SELECT column_name as col, 'target' as src 
                    FROM \`${targetTable.split('.')[0]}.${targetTable.split('.')[1]}\`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '${targetTable.split('.')[2]}'
                )
            `;
            const [schemaCols] = await bigquery.query(schemaQuery);
            schemaCols.forEach(r => {
                if (r.src === 'source') sourceFields.push(r.col);
                else targetFields.push(r.col);
            });
        } catch (schemaErr) {
            try {
                const [srcSample] = await bigquery.query(`SELECT * FROM \`${sourceTable}\` LIMIT 1`);
                const [tgtSample] = await bigquery.query(`SELECT * FROM \`${targetTable}\` LIMIT 1`);
                if (srcSample.length > 0) sourceFields = Object.keys(srcSample[0]);
                if (tgtSample.length > 0) targetFields = Object.keys(tgtSample[0]);
            } catch (e) {
                return res.status(400).json({ success: false, error: `Cannot read schemas: ${e.message}` });
            }
        }

        const commonFields = sourceFields.filter(f => targetFields.includes(f));
        const sourceOnlyFields = sourceFields.filter(f => !targetFields.includes(f));
        const targetOnlyFields = targetFields.filter(f => !sourceFields.includes(f));

        let fieldsToCompare;
        if (comparisonFields.length > 0) {
            fieldsToCompare = comparisonFields.filter(f => commonFields.includes(f));
        } else {
            fieldsToCompare = commonFields.filter(f => f !== primaryKey);
        }

        // STEP 5: Record matching (sample-based)
        console.log('Matching records (sample-based)...');
        const matchQuery = `
            WITH source_sample AS (${sampleSubquery}),
            target_keys AS (
                SELECT DISTINCT SAFE_CAST(${primaryKey} AS STRING) as pk
                FROM \`${targetTable}\`
                WHERE ${primaryKey} IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM source_sample s INNER JOIN target_keys t ON s.pk = t.pk) as matched,
                (SELECT COUNT(*) FROM source_sample s LEFT JOIN target_keys t ON s.pk = t.pk WHERE t.pk IS NULL) as source_only,
                (SELECT COUNT(*) FROM target_keys t LEFT JOIN source_sample s ON t.pk = s.pk WHERE s.pk IS NULL) as target_only
        `;

        let matchCounts;
        try {
            const [matchRows] = await bigquery.query(matchQuery);
            matchCounts = matchRows[0];
        } catch (matchErr) {
            return res.status(400).json({ success: false, error: `Match query failed: ${matchErr.message}` });
        }

        // STEP 6: Field-by-field comparison (sample-based)
        console.log('Field comparison (sample-based)...');
        const fieldComparisons = [];
        let totalFieldIssues = 0;
        let perfectFieldCount = 0;

        const FIELD_BATCH_SIZE = 5;
        for (let i = 0; i < fieldsToCompare.length; i += FIELD_BATCH_SIZE) {
            const fieldBatch = fieldsToCompare.slice(i, i + FIELD_BATCH_SIZE);

            const fieldSelectParts = fieldBatch.map(field => `
                COUNTIF(SAFE_CAST(s.${field} AS STRING) = SAFE_CAST(t.${field} AS STRING) OR (s.${field} IS NULL AND t.${field} IS NULL)) as match_${field.replace(/[^a-zA-Z0-9]/g, '_')},
                COUNTIF(NOT (SAFE_CAST(s.${field} AS STRING) = SAFE_CAST(t.${field} AS STRING) OR (s.${field} IS NULL AND t.${field} IS NULL))) as diff_${field.replace(/[^a-zA-Z0-9]/g, '_')}
            `).join(',\n');

            const fieldCompareQuery = `
                WITH source_sample AS (${sampleSubquery})
                SELECT 
                    COUNT(*) as total_compared,
                    ${fieldSelectParts}
                FROM \`${sourceTable}\` s
                INNER JOIN \`${targetTable}\` t
                ON SAFE_CAST(s.${primaryKey} AS STRING) = SAFE_CAST(t.${primaryKey} AS STRING)
                WHERE SAFE_CAST(s.${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)
            `;

            try {
                const [fieldRows] = await bigquery.query(fieldCompareQuery);
                const row = fieldRows[0];
                const totalCompared = row.total_compared || 0;

                fieldBatch.forEach(field => {
                    const safeField = field.replace(/[^a-zA-Z0-9]/g, '_');
                    const matches = row[`match_${safeField}`] || 0;
                    const diffs = row[`diff_${safeField}`] || 0;
                    const matchRate = totalCompared > 0 ? ((matches / totalCompared) * 100).toFixed(1) : '0.0';

                    if (diffs > 0) totalFieldIssues += diffs;
                    else perfectFieldCount++;

                    fieldComparisons.push({
                        fieldName: field, totalRecords: totalCompared,
                        perfectMatches: matches, differences: diffs,
                        matchRate: matchRate, error: null
                    });
                });
            } catch (fieldErr) {
                console.warn(`Field batch failed:`, fieldErr.message);
                fieldBatch.forEach(field => {
                    fieldComparisons.push({
                        fieldName: field, totalRecords: 0, perfectMatches: 0,
                        differences: 0, matchRate: '0.0', error: fieldErr.message
                    });
                });
            }
        }

        // STEP 7: Duplicate detection (sample-based)
        let sourceDupKeys = [], targetDupKeys = [];
        try {
            const dupQuery = `
                WITH source_sample AS (${sampleSubquery}),
                source_dups AS (
                    SELECT SAFE_CAST(${primaryKey} AS STRING) as pk, COUNT(*) as cnt
                    FROM \`${sourceTable}\`
                    WHERE ${primaryKey} IS NOT NULL
                    AND SAFE_CAST(${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)
                    GROUP BY pk HAVING cnt > 1
                    ORDER BY cnt DESC LIMIT 20
                ),
                target_dups AS (
                    SELECT SAFE_CAST(${primaryKey} AS STRING) as pk, COUNT(*) as cnt
                    FROM \`${targetTable}\`
                    WHERE ${primaryKey} IS NOT NULL
                    AND SAFE_CAST(${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)
                    GROUP BY pk HAVING cnt > 1
                    ORDER BY cnt DESC LIMIT 20
                )
                SELECT pk, cnt, 'source' as src FROM source_dups
                UNION ALL
                SELECT pk, cnt, 'target' as src FROM target_dups
            `;
            const [dupRows] = await bigquery.query(dupQuery);
            dupRows.forEach(r => {
                if (r.src === 'source') sourceDupKeys.push({ key: r.pk, count: r.cnt });
                else targetDupKeys.push({ key: r.pk, count: r.cnt });
            });
        } catch (dupErr) {
            console.warn('Duplicate detection failed:', dupErr.message);
        }

        // STEP 8: Sample differences
        let sampleDiffs = [];
        if (fieldsToCompare.length > 0 && matchCounts.matched > 0) {
            try {
                const firstField = fieldsToCompare[0];
                const sampleDiffQuery = `
                    WITH source_sample AS (${sampleSubquery})
                    SELECT 
                        SAFE_CAST(s.${primaryKey} AS STRING) as record_key,
                        SAFE_CAST(s.${firstField} AS STRING) as source_value,
                        SAFE_CAST(t.${firstField} AS STRING) as target_value,
                        '${firstField}' as field_name
                    FROM \`${sourceTable}\` s
                    INNER JOIN \`${targetTable}\` t
                    ON SAFE_CAST(s.${primaryKey} AS STRING) = SAFE_CAST(t.${primaryKey} AS STRING)
                    WHERE SAFE_CAST(s.${primaryKey} AS STRING) IN (SELECT pk FROM source_sample)
                    AND SAFE_CAST(s.${firstField} AS STRING) != SAFE_CAST(t.${firstField} AS STRING)
                    LIMIT 5
                `;
                const [diffRows] = await bigquery.query(sampleDiffQuery);
                sampleDiffs = diffRows;
            } catch (e) {
                console.warn('Sample diff failed:', e.message);
            }
        }

        // BUILD RESPONSE
        const successRate = sampleValidated > 0
            ? ((matchCounts.matched / sampleValidated) * 100).toFixed(1) : '0.0';

        const sampleSourceDupTotal = sourceDupKeys.reduce((sum, d) => sum + d.count, 0);
        const sampleTargetDupTotal = targetDupKeys.reduce((sum, d) => sum + d.count, 0);
        const bothClean = sourceDupKeys.length === 0 && targetDupKeys.length === 0;
        const maxDiffs = fieldComparisons.reduce((max, f) => Math.max(max, f.differences || 0), 0);

        const response = {
            success: true,
            data: {
                summary: {
                    totalRecordsInFile: sourceTotalCount,
                    totalRecordsInSource: sourceTotalCount,
                    targetRecords: targetTotalCount,
                    uniqueSourceRecords: sampleSourceStats.unique,
                    duplicateRecordsInFile: sampleSourceStats.duplicates,
                    recordsReachedTarget: matchCounts.matched,
                    recordsFailedToReachTarget: matchCounts.source_only,
                    recordsOnlyInTarget: matchCounts.target_only,
                    identicalRecords: Math.max(0, matchCounts.matched - maxDiffs),
                    mismatchedRecords: Math.min(matchCounts.matched, maxDiffs),
                    pipelineSuccessRate: successRate,
                    primaryKeyUsed: primaryKey,
                    fieldsAnalyzed: fieldsToCompare.length,
                    commonFieldsCount: commonFields.length,
                    schemaCompatibility: ((commonFields.length / Math.max(sourceFields.length, targetFields.length, 1)) * 100).toFixed(1),
                    totalFieldIssues: totalFieldIssues,
                    nullPrimaryKeysSource: 0,
                    nullPrimaryKeysTarget: 0,
                    sampleSize: SAMPLE_SIZE,
                    sampleValidated: sampleValidated,
                    isSampleBased: true,
                    samplingNote: `Full source: ${sourceTotalCount.toLocaleString()} records. Validated ${sampleValidated.toLocaleString()} sample records.`
                },
                recordCounts: {
                    jsonDetails: {
                        totalRecords: sourceTotalCount,
                        uniquePrimaryKeys: sampleSourceStats.unique,
                        duplicateRecords: sampleSourceStats.duplicates,
                        nullPrimaryKeys: 0,
                        primaryKeyField: primaryKey
                    },
                    bqDetails: {
                        totalRecords: targetTotalCount,
                        uniquePrimaryKeys: sampleTargetStats.unique,
                        duplicateRecords: sampleTargetStats.duplicates,
                        nullPrimaryKeys: 0
                    }
                },
                schemaAnalysis: {
                    totalJsonFields: sourceFields.length,
                    totalBqFields: targetFields.length,
                    commonFields: commonFields,
                    jsonOnlyFields: sourceOnlyFields,
                    bqOnlyFields: targetOnlyFields,
                    schemaCompatibility: ((commonFields.length / Math.max(sourceFields.length, targetFields.length, 1)) * 100).toFixed(1),
                    primaryKeyCandidates: commonFields.filter(f => 
                        f.toLowerCase().includes('id') || f.toLowerCase().includes('key')
                    )
                },
                fieldWiseAnalysis: {
                    fieldsAnalyzed: fieldsToCompare.length,
                    perfectFields: perfectFieldCount,
                    problematicFields: fieldsToCompare.length - perfectFieldCount,
                    totalFieldIssues: totalFieldIssues,
                    recordsAnalyzed: matchCounts.matched,
                    fieldComparison: fieldComparisons
                },
                duplicatesAnalysis: {
                    jsonDuplicates: {
                        duplicateCount: sourceDupKeys.length,
                        totalDuplicateRecords: sampleSourceDupTotal,
                        duplicateKeys: sourceDupKeys
                    },
                    bqDuplicates: {
                        duplicateCount: targetDupKeys.length,
                        totalDuplicateRecords: sampleTargetDupTotal,
                        duplicateKeys: targetDupKeys
                    },
                    crossSystemAnalysis: {
                        commonDuplicateKeys: sourceDupKeys
                            .filter(s => targetDupKeys.some(t => t.key === s.key))
                            .map(s => s.key)
                    },
                    summary: {
                        bothSystemsClean: bothClean,
                        dataQualityScore: bothClean ? 'Excellent' :
                            (sourceDupKeys.length + targetDupKeys.length < 10) ? 'Good' : 'Needs Review'
                    },
                    recommendations: bothClean ? [] : [
                        sourceDupKeys.length > 0 ? `Source has ${sourceDupKeys.length} duplicate keys in sample` : null,
                        targetDupKeys.length > 0 ? `Target has ${targetDupKeys.length} duplicate keys for sample PKs` : null
                    ].filter(Boolean)
                },
                metadata: {
                    sourceType: 'BIGQUERY',
                    sourceTable: sourceTable,
                    targetTable: targetTable,
                    primaryKey: primaryKey,
                    comparisonType: 'BQ-vs-BQ',
                    sampleSize: SAMPLE_SIZE,
                    sampleValidated: sampleValidated,
                    sourceFilter: sourceFilter || 'None',
                    comparedAt: new Date().toISOString()
                }
            }
        };

        console.log(`BQ vs BQ completed - Source: ${sourceTotalCount}, Target: ${targetTotalCount}, Sample: ${sampleValidated} validated`);
        res.json(response);

    } catch (error) {
        console.error('BQ vs BQ comparison failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            suggestions: [
                'Check both table names (project.dataset.table)',
                'Verify primary key exists in both tables',
                'Ensure read access to both tables',
                'Check source filter syntax'
            ]
        });
    }
});

// Schema-aware null check endpoint (from QE_Consol_QA)
app.post('/api/bq-schema-null-check', async (req, res) => {
    const { compareSchemas, identifyNotNullColumns, buildNullValidationResults, validateRequestParams } = require('./services/schema-null-check');

    const validation = validateRequestParams(req.body);
    if (!validation.valid) {
        return res.status(400).json({ success: false, error: validation.error });
    }

    const { sourceTable, targetTable, primaryKey } = req.body;

    const [srcProject, srcDataset, srcTableName] = sourceTable.split('.');
    const [tgtProject, tgtDataset, tgtTableName] = targetTable.split('.');

    let sourceColumns, targetColumns;

    try {
        const [rows] = await bigquery.query({
            query: `SELECT column_name, data_type, is_nullable FROM \`${srcProject}.${srcDataset}\`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${srcTableName}' ORDER BY ordinal_position`,
            location: 'US',
        });
        sourceColumns = rows;
    } catch (error) {
        return res.status(400).json({
            success: false,
            error: `Failed to read schema for source table ${sourceTable}: ${error.message}`,
        });
    }

    try {
        const [rows] = await bigquery.query({
            query: `SELECT column_name, data_type, is_nullable FROM \`${tgtProject}.${tgtDataset}\`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '${tgtTableName}' ORDER BY ordinal_position`,
            location: 'US',
        });
        targetColumns = rows;
    } catch (error) {
        return res.status(400).json({
            success: false,
            error: `Failed to read schema for target table ${targetTable}: ${error.message}`,
        });
    }

    const schemaComparison = compareSchemas(sourceColumns, targetColumns);
    const notNullColumns = identifyNotNullColumns(sourceColumns, primaryKey);

    let nullValidation = { notNullColumns, results: [], summary: { totalColumnsValidated: 0, columnsPassed: 0, columnsFailed: 0, passRate: '0.0' } };

    if (notNullColumns.length > 0) {
        try {
            const countifClauses = notNullColumns
                .map((col) => `COUNTIF(\`${col}\` IS NULL) as null_${col}`)
                .join(', ');
            const nullQuery = `SELECT COUNT(*) as total_rows, ${countifClauses} FROM \`${targetTable}\``;

            const [rows] = await bigquery.query({ query: nullQuery, location: 'US' });
            const nullCountRow = rows[0];
            const totalRows = parseInt(nullCountRow.total_rows, 10) || 0;

            nullValidation = {
                notNullColumns,
                ...buildNullValidationResults(notNullColumns, nullCountRow, totalRows),
            };
        } catch (error) {
            return res.status(500).json({
                success: false,
                error: `Null count query failed: ${error.message}`,
            });
        }
    }

    res.json({
        success: true,
        data: {
            schemaComparison,
            nullValidation,
            metadata: {
                sourceTable,
                targetTable,
                primaryKey,
                checkedAt: new Date().toISOString(),
            },
        },
    });
});

// Health Check Endpoint
app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '1.0.0',
        services: {
            bigquery: 'connected',
            oracle: 'ready'
        }
    });
});


// ==================== API vs BQ ENDPOINTS ====================
app.post('/api/test-api-connection', async (req, res) => {
    try {
        const { url, method, headers, body, username, password, authType } = req.body;

        // VALIDATION: Block PUT and PATCH methods
        if (method && ['PUT', 'PATCH'].includes(method.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'PUT and PATCH methods are temporarily disabled',
                details: 'Please use GET or POST methods for API testing',
                suggestions: [
                    'Use GET method for data retrieval',
                    'Use POST method for data submission or authentication',
                    'Contact administrator if PUT/PATCH access is required'
                ]
            });
        }

        // ... rest of existing code continues unchanged
        if (!url) {
            return res.status(400).json({ success: false, error: 'URL required' });
        }

        console.log('Testing API connection with method:', method || 'GET');

        const result = await apiFetcher.testAPIConnection({
            url,
            method: method || 'GET',  // Pass method to service
            headers: headers || {},
            body: body || null,       // Pass body to service
            username,
            password,
            authType
        });

        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/fetch-api-data', async (req, res) => {
    try {
        const { url, method, headers, body, username, password, authType } = req.body;

        // VALIDATION: Block PUT and PATCH methods
        if (method && ['PUT', 'PATCH'].includes(method.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'PUT and PATCH methods are temporarily disabled',
                details: 'Please use GET or POST methods for API data fetching'
            });
        }

        // ... rest of existing code continues unchanged
        console.log('=== ENHANCED API DATA FETCH (RESPECTS USER PAGINATION) ===');
        console.log(`URL: ${url}`);
        console.log(`Method: ${method || 'GET'}`);

        if (!url) {
            return res.status(400).json({
                success: false,
                error: 'URL required for API fetch'
            });
        }

        // Check if URL has user-specified pagination parameters
        const urlObj = new URL(url);
        const hasUserPagination = urlObj.searchParams.has('per_page') ||
                                  urlObj.searchParams.has('limit') ||
                                  urlObj.searchParams.has('page_size') ||
                                  urlObj.searchParams.has('page') ||
                                  urlObj.searchParams.has('offset');

        if (hasUserPagination) {
            console.log('USER PAGINATION DETECTED in URL:', url);
            console.log('User parameters:', Object.fromEntries(urlObj.searchParams.entries()));
            console.log('Will respect user pagination exactly');
        }

        // Build configuration
        const config = {
            url,
            method: method || 'GET',
            headers: headers || {},
            body,
            username,
            password,
            authType,
            respectUserPagination: hasUserPagination // Flag for the service
        };

        console.log('Calling APIFetcherService with enhanced pagination respect...');

        const APIFetcherService = require('./services/api-fetcher');
        const apiFetcher = new APIFetcherService();

        // Use the updated fetchAPIData method
        const result = await apiFetcher.fetchAPIData(config);

        if (result.success) {
            console.log('API FETCH SUCCESS:');
            console.log(`- Records fetched: ${result.metadata?.totalRecords || 'unknown'}`);
            console.log(`- Strategy used: ${result.metadata?.fetchStrategy || 'standard'}`);
            console.log(`- User pagination respected: ${result.userPaginationRespected || false}`);

            // Add debugging info for user pagination
            if (hasUserPagination) {
                result.debugInfo = {
                    userPaginationDetected: true,
                    userParameters: Object.fromEntries(urlObj.searchParams.entries()),
                    strategyUsed: result.metadata?.fetchStrategy || 'direct-user-request'
                };
            }
        }

        res.json(result);

    } catch (error) {
        console.error('Enhanced API fetch failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Enhanced API fetch with user pagination respect failed'
        });
    }
});
// REMOVE THIS ENTIRE BROKEN SECTION:
// server.js - ADD THIS NEW ENDPOINT (insert after the existing /api/fetch-api-data endpoint)



app.get('/api/preview-api/:dataId', async (req, res) => {
    try {
        const result = await apiFetcher.getAPIDataPreview(req.params.dataId);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/create-temp-table-from-api', async (req, res) => {
    try {
        const { dataId, primaryKey } = req.body;
        const apiDataResult = await apiFetcher.getAPIData(dataId);
        if (!apiDataResult.success) return res.status(404).json({ success: false, error: 'API data not found' });

        let jsonData = apiDataResult.data.result || apiDataResult.data;
        jsonData = expandUUIDKeyedData(jsonData);

        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(jsonData, dataId, primaryKey);

        res.json({ success: true, tempTableId: result.tempTableId, recordsUploaded: result.recordsInTable });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/compare-api-vs-bq', async (req, res) => {
    try {
        const { dataId, sourceTable, primaryKey } = req.body;

        const apiDataResult = await apiFetcher.getAPIData(dataId);
        let jsonData = apiDataResult.data.result || apiDataResult.data;
        jsonData = expandUUIDKeyedData(jsonData);

        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(jsonData, dataId, primaryKey);

        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        const results = await comparisonEngine.compareJSONvsBigQuery(tempTableResult.tempTableId, sourceTable, primaryKey, []);

        results.metadata.dataSource = 'API';
        res.json(results);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// server.js - ADD THESE MISSING ENDPOINTS (add after existing API endpoints, before app.listen)

// MISSING ENDPOINT 1: Enhanced API vs BQ comprehensive comparison
app.post('/api/compare-api-vs-bq-comprehensive', async (req, res) => {
    try {
        const {
            dataId,
            sourceTable,
            primaryKey,
            comparisonFields = [],
            includeFieldAnalysis = true,
            includeDuplicateAnalysis = true,
            includeSchemaAnalysis = true,
            bqFilter = null, // Optional BigQuery filter condition
            explodeArrayField = null // NEW: Optional field name to explode nested arrays (e.g., 'metrics')
        } = req.body;

        console.log(`COMPREHENSIVE API vs BQ comparison with BigQuery filtering starting...`);
        console.log(`DataId: ${dataId}`);
        console.log(`Source table: ${sourceTable}`);
        console.log(`Primary key: ${primaryKey}`);
        console.log(`BigQuery filter: ${bqFilter || 'None (compare all records)'}`);
        console.log(`Explode array field: ${explodeArrayField || 'None (standard flattening)'}`); // NEW

        if (!dataId || !sourceTable || !primaryKey) {
            return res.status(400).json({
                success: false,
                error: 'dataId, sourceTable, and primaryKey are required for comprehensive comparison'
            });
        }

        // NEW: Validate BigQuery filter syntax if provided
        if (bqFilter && bqFilter.trim()) {
            const filterValidation = validateBigQueryFilter(bqFilter.trim());
            if (!filterValidation.isValid) {
                return res.status(400).json({
                    success: false,
                    error: 'Invalid filter condition',
                    details: filterValidation.error,
                    suggestions: [
                        'Use standard SQL WHERE clause syntax',
                        'Example: account_id = \'4ad8c07d3053ca49828dbd7d626b66cf\'',
                        'Example: status IN (\'active\', \'enabled\')',
                        'Example: created_date >= \'2024-01-01\'',
                        'Ensure field names exist in your BigQuery table'
                    ]
                });
            }
        }

        // Get API data
        const apiDataResult = await apiFetcher.getAPIData(dataId);
        if (!apiDataResult.success) {
            return res.status(404).json({
                success: false,
                error: 'API data not found or expired',
                details: apiDataResult.error
            });
        }

        // Extract actual data from various API response structures
        // FIRST: Check for Azure App Insights tabular format (tables/columns/rows)
        let jsonData = parseTabularFormat(apiDataResult.data);
        
        if (!jsonData) {
            // Standard extraction for other API formats
            jsonData = apiDataResult.data.results  // ServiceNow/common pattern (plural)
                        || apiDataResult.data.result   // Alternative pattern (singular)
                        || apiDataResult.data.data     // Nested data pattern
                        || apiDataResult.data.records  // Records pattern
                        || apiDataResult.data.items    // Items pattern
                        || apiDataResult.data;         // Direct data fallback
            
            // Normalize data: expand keyed objects
            // NOTE: We do NOT apply camelToSnake here because BQ tables typically use
            // concatenated lowercase (e.g., "projectnumber" not "project_number").
            // Field name matching is handled by the comparison engine's fuzzy matching.
            if (Array.isArray(jsonData)) {
                // Keep field names as-is from the API (preserve original casing)
                // The comparison engine handles case-insensitive + underscore-insensitive matching
            } else {
                // For non-array responses, use expandUUIDKeyedData for structure detection
                // but it will apply camelToSnake - we accept this for UUID-keyed formats
                jsonData = expandUUIDKeyedData(jsonData);
            }
        } else {
            console.log(`Tabular format parsed successfully - skipping expandUUIDKeyedData`);
        }
        
        // Ensure jsonData is always an array
        if (!Array.isArray(jsonData)) {
            jsonData = [jsonData];
        }
        
        // Debug: log field names after conversion
        if (jsonData.length > 0) {
            console.log(`After expandUUIDKeyedData - fields: [${Object.keys(jsonData[0]).join(', ')}]`);
        }
        
        // Filter out wrapper objects that don't contain actual record data
        if (jsonData.length === 1 && jsonData[0].results && Array.isArray(jsonData[0].results)) {
            console.log('Detected wrapper object, extracting results array...');
            jsonData = jsonData[0].results;
        }
        
        // Auto-detect wrapper objects with a single array field as the main data
        // e.g., { "rackspace_accounts": [{...}, {...}] } → extract the array
        if (jsonData.length === 1 && typeof jsonData[0] === 'object' && !Array.isArray(jsonData[0])) {
            const keys = Object.keys(jsonData[0]);
            // Find fields that contain arrays of objects (candidate data arrays)
            const arrayFields = keys.filter(k => {
                const val = jsonData[0][k];
                return Array.isArray(val) && val.length > 0 && typeof val[0] === 'object' && val[0] !== null;
            });
            
            if (arrayFields.length === 1) {
                // Single array field detected — this IS the data
                const fieldName = arrayFields[0];
                console.log(`Auto-detected wrapper with single data array field: '${fieldName}' (${jsonData[0][fieldName].length} records)`);
                jsonData = jsonData[0][fieldName];
            } else if (arrayFields.length > 1) {
                // Multiple array fields — pick the largest one
                const largestField = arrayFields.reduce((a, b) => 
                    jsonData[0][a].length >= jsonData[0][b].length ? a : b
                );
                console.log(`Auto-detected wrapper with multiple arrays, using largest: '${largestField}' (${jsonData[0][largestField].length} records)`);
                jsonData = jsonData[0][largestField];
            }
        }

        console.log(`API data retrieved: ${jsonData.length} records`);
        
        // NEW: Explode nested array if specified (e.g., 'metrics' array)
        if (explodeArrayField && explodeArrayField.trim()) {
            console.log(`=== EXPLODING NESTED ARRAY: ${explodeArrayField} ===`);
            const originalCount = jsonData.length;
            jsonData = explodeNestedArray(jsonData, explodeArrayField.trim());
            console.log(`Array explosion: ${originalCount} records GÃ¥Ã† ${jsonData.length} records`);
            console.log(`Each nested ${explodeArrayField} item is now a separate row with parent fields preserved`);
        }
        
        if (apiDataResult.metadata?.comparisonStrategy === 'first-page-with-total-count') {
            console.log('DETECTED: All records strategy was used');
            console.log(`Total API records: ${apiDataResult.metadata.totalRecordsInAPI}`);
            console.log(`Records for comparison: ${apiDataResult.metadata.recordsForComparison}`);
        }

        // Flatten the data same as JSON processing
        const flattenedData = jsonData.map((record) => {
            const flattened = {};

            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;

                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        if (value.display_value || value.link || value.value) {
                            if (value.display_value) {
                                flattened[`${newKey}_display_value`] = String(value.display_value);
                            }
                            if (value.link) {
                                flattened[`${newKey}_link`] = String(value.link);
                            }
                            if (value.value) {
                                flattened[`${newKey}_value`] = String(value.value);
                            }
                        } else {
                            if (prefix.split('_').length < 3) {
                                flattenObject(value, newKey);
                            } else {
                                flattened[newKey] = JSON.stringify(value);
                            }
                        }
                    } else if (Array.isArray(value)) {
                        flattened[newKey] = JSON.stringify(value);
                    } else {
                        if (value === null || value === undefined) {
                            flattened[newKey] = null;
                        } else {
                            flattened[newKey] = String(value);
                        }
                    }
                }
            }

            flattenObject(record);
            return flattened;
        });

        console.log(`Data flattened for BigQuery compatibility`);

        // Create temp table from API data
        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(flattenedData, dataId, primaryKey);

        if (!tempTableResult.success) {
            throw new Error(`Failed to create temp table from API data: ${tempTableResult.error}`);
        }

        console.log(`Temp table created: ${tempTableResult.tempTableId}`);

        // NEW: Enhanced comparison engine call with BigQuery filter
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();

        console.log(`Running comprehensive comparison with BigQuery filtering...`);

        // Pass the BigQuery filter and unnest field to the comparison engine
        // When explodeArrayField is set, BigQuery needs to UNNEST the same field for proper comparison
        const results = await comparisonEngine.compareJSONvsBigQueryWithFilter(
            tempTableResult.tempTableId,
            sourceTable,
            primaryKey,
            comparisonFields,
            'enhanced',
            bqFilter, // BigQuery filter condition
            explodeArrayField // NEW: Pass unnest field for BigQuery nested array support
        );

        // Check if comparison failed
        if (!results.success && results.error) {
            console.error(`API vs BQ comparison failed: ${results.error}`);
            return res.status(400).json({
                success: false,
                error: results.error,
                details: 'Comparison failed - please check your primary key and table configuration',
                suggestions: [
                    `Verify the primary key '${primaryKey}' exists in your API data`,
                    'Check that the source table name is correct',
                    'Ensure the BigQuery filter syntax is valid if using filtering'
                ],
                filterInformation: results.filterInformation,
                metadata: results.metadata
            });
        }

        console.log(`API vs BQ comprehensive comparison with filtering completed successfully`);

        // Enhanced metadata with filter information
        results.metadata = {
            ...(results.metadata || {}),
            dataSource: 'API',
            apiUrl: apiDataResult.metadata?.url || 'unknown',
            authType: apiDataResult.metadata?.authenticationUsed || 'unknown',
            responseTime: apiDataResult.metadata?.duration || 0,
            authenticationStatus: 'success',

            // NEW: BigQuery filter information
            bigQueryFilter: {
                applied: !!(bqFilter && bqFilter.trim()),
                condition: bqFilter && bqFilter.trim() ? bqFilter.trim() : null,
                description: bqFilter && bqFilter.trim()
                    ? `Filtered BigQuery data using: ${bqFilter.trim()}`
                    : 'No BigQuery filtering applied - comparing all records'
            },
            
            // NEW: Array explosion information
            arrayExplosion: {
                applied: !!(explodeArrayField && explodeArrayField.trim()),
                field: explodeArrayField && explodeArrayField.trim() ? explodeArrayField.trim() : null,
                description: explodeArrayField && explodeArrayField.trim()
                    ? `Exploded nested array '${explodeArrayField.trim()}' - each array item is now a separate row`
                    : 'No array explosion applied - standard flattening used'
            },

            // All records strategy metadata
            allRecordsStrategy: {
                used: apiDataResult.metadata?.comparisonStrategy === 'first-page-with-total-count',
                totalRecordsInAPI: apiDataResult.metadata?.totalRecordsInAPI,
                recordsForComparison: apiDataResult.metadata?.recordsForComparison,
                strategy: apiDataResult.metadata?.comparisonStrategy || 'standard'
            }
        };

        // Update summary to include filter information and total records
        if (results.summary) {
            if (apiDataResult.metadata?.totalRecordsInAPI) {
                results.summary.totalRecordsInAPI = apiDataResult.metadata.totalRecordsInAPI;
                results.summary.recordsUsedForComparison = apiDataResult.metadata.recordsForComparison;
                results.summary.allRecordsStrategy = 'enabled';
            }

            // NEW: Add filter information to summary
            results.summary.bigQueryFilterApplied = !!(bqFilter && bqFilter.trim());
            results.summary.bigQueryFilterCondition = bqFilter && bqFilter.trim() ? bqFilter.trim() : null;
            
            // NEW: Add array explosion info to summary
            results.summary.arrayExplosionApplied = !!(explodeArrayField && explodeArrayField.trim());
            results.summary.arrayExplosionField = explodeArrayField && explodeArrayField.trim() ? explodeArrayField.trim() : null;
        }

        // Update schema analysis to reflect API source
        if (results.schemaAnalysis) {
            results.schemaAnalysis.apiOnlyFields = results.schemaAnalysis.jsonOnlyFields;
            delete results.schemaAnalysis.jsonOnlyFields;
            results.schemaAnalysis.totalApiFields = results.schemaAnalysis.totalJsonFields;
            delete results.schemaAnalysis.totalJsonFields;
        }

        // Update duplicates analysis to reflect API source
        if (results.duplicatesAnalysis) {
            results.duplicatesAnalysis.apiDuplicates = results.duplicatesAnalysis.jsonDuplicates;
            delete results.duplicatesAnalysis.jsonDuplicates;
        }

        // Update record counts to reflect API source
        if (results.recordCounts) {
            results.recordCounts.apiDetails = results.recordCounts.jsonDetails;
            delete results.recordCounts.jsonDetails;
        }

        results.success = true;
        results.primaryKeyUsed = primaryKey;

        res.json(results);

    } catch (error) {
        console.error('Comprehensive API vs BQ comparison with filtering failed:', error.message);

        let errorMessage = error.message;
        let suggestions = [
            'Check that the primary key field exists in both API data and BigQuery table',
            'Verify BigQuery table is accessible',
            'Try using a different field that exists in both systems'
        ];

        // NEW: Enhanced error handling for filter-related issues
        if (error.message.includes('Invalid filter condition')) {
            suggestions = [
                'Check your BigQuery filter syntax - use standard SQL WHERE clause format',
                'Example: account_id = \'4ad8c07d3053ca49828dbd7d626b66cf\'',
                'Example: status IN (\'active\', \'enabled\') AND region = \'us-east\'',
                'Example: created_date >= \'2024-01-01\'',
                'Ensure all field names in the filter exist in your BigQuery table',
                'Field names are case-sensitive'
            ];
        } else if (error.message.includes('Filter field not found')) {
            suggestions = [
                'One or more fields in your BigQuery filter do not exist in the table',
                'Check the Column Names tab to see available BigQuery fields',
                'Ensure field names match exactly (case-sensitive)',
                'Remove the filter or correct the field names'
            ];
        } else if (error.message.includes('not available in both tables')) {
            suggestions = [
                'Choose a field that exists in both your API data and BigQuery table',
                'Check the Column Names tab to see available common fields',
                'API supports any data type - the issue is field name mismatch'
            ];
        } else if (error.message.includes('No records match filter')) {
            suggestions = [
                'Your BigQuery filter condition returned no matching records',
                'Try a less restrictive filter condition',
                'Verify your filter values exist in the BigQuery table',
                'Remove the filter to compare all BigQuery records'
            ];
        }

        res.status(500).json({
            success: false,
            error: errorMessage,
            details: 'Comprehensive API vs BQ comparison with BigQuery filtering failed',
            suggestions: suggestions,
            filterApplied: !!(req.body.bqFilter && req.body.bqFilter.trim()),
            filterCondition: req.body.bqFilter && req.body.bqFilter.trim() ? req.body.bqFilter.trim() : null
        });
    }
});

// MISSING ENDPOINT 3: Create temp table from API data (simplified version)
app.post('/api/create-temp-table-from-api', async (req, res) => {
    try {
        const { dataId, primaryKey } = req.body;

        console.log(`Creating temp table from API data: ${dataId}`);

        const apiDataResult = await apiFetcher.getAPIData(dataId);
        if (!apiDataResult.success) {
            return res.status(404).json({ success: false, error: 'API data not found' });
        }

        let jsonData = apiDataResult.data.result || apiDataResult.data;
        jsonData = expandUUIDKeyedData(jsonData);

        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(jsonData, dataId, primaryKey);

        res.json({
            success: true,
            tempTableId: result.tempTableId,
            recordsUploaded: result.recordsInTable,
            message: result.message
        });

    } catch (error) {
        console.error('Create temp table from API failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
// Start serv// Add this debug route right before app.listen()
app.get('/debug-structure', (req, res) => {
    const fs = require('fs');
    const publicPath = path.join(__dirname, 'public');
    const htmlPath = path.join(__dirname, 'public', 'index.html');

    res.json({
        __dirname: __dirname,
        publicPath: publicPath,
        htmlPath: htmlPath,
        publicExists: fs.existsSync(publicPath),
        htmlExists: fs.existsSync(htmlPath),
        publicFiles: fs.existsSync(publicPath) ? fs.readdirSync(publicPath) : [],
        rootFiles: fs.readdirSync(__dirname).slice(0, 20)
    });
});

function validateBigQueryFilter(filterCondition) {
    try {
        // Basic syntax validation
        if (!filterCondition || filterCondition.trim() === '') {
            return { isValid: true }; // Empty filter is valid (means no filtering)
        }

        const trimmed = filterCondition.trim();

        // Check for dangerous SQL injection patterns
        const dangerousPatterns = [
            /;\s*(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER)\s/i,
            /--/,  // SQL comments
            /\/\*.*?\*\//,  // Block comments
            /\bUNION\s+SELECT\b/i,
            /\bEXEC\s*\(/i
        ];

        for (const pattern of dangerousPatterns) {
            if (pattern.test(trimmed)) {
                return {
                    isValid: false,
                    error: 'Filter contains potentially unsafe SQL patterns'
                };
            }
        }

        // Basic structure validation - should look like a WHERE clause
        const validPatterns = [
            /\w+\s*(=|!=|<>|>|<|>=|<=|LIKE|IN|NOT IN|IS|IS NOT)\s*[\w'"\(\)]/i,
            /\w+\s+(AND|OR)\s+\w+/i
        ];

        const hasValidStructure = validPatterns.some(pattern => pattern.test(trimmed));

        if (!hasValidStructure && trimmed.length > 0) {
            return {
                isValid: false,
                error: 'Filter does not appear to be a valid WHERE clause condition'
            };
        }

        return { isValid: true };

    } catch (error) {
        return {
            isValid: false,
            error: `Filter validation error: ${error.message}`
        };
    }
}

// Proxy API Request endpoint for multi-API functionality
app.post('/api/proxy-api-request', async (req, res) => {
    try {
        const { url, method = 'GET', headers = {}, body } = req.body;
        
        if (!url) {
            return res.status(400).json({ success: false, error: 'URL is required' });
        }
        
        console.log(`Proxying ${method} request to: ${url}`);
        console.log('Headers being sent:', JSON.stringify(headers, null, 2));
        
        const axios = require('axios');
        
        const axiosConfig = {
            method: method,
            url: url,
            headers: headers,
            timeout: 60000
        };
        
        if (body && ['POST', 'PUT', 'PATCH'].includes(method.toUpperCase())) {
            axiosConfig.data = body;
        }
        
        const response = await axios(axiosConfig);
        
        console.log('API Response status:', response.status);
        res.json({ success: true, data: response.data, status: response.status });
        
    } catch (error) {
        console.error('Proxy API request failed:', error.message);
        console.error('Error response data:', error.response?.data);
        const errorMsg = error.response?.data?.errors?.[0]?.message || error.response?.data?.message || error.message;
        res.status(error.response?.status || 500).json({ 
            success: false, 
            error: errorMsg,
            details: error.response?.data 
        });
    }
});

// API vs BQ Compare endpoint for multi-API functionality
app.post('/api/api-vs-bq-compare', async (req, res) => {
    try {
        const { apiData, bqTable, primaryKey, bqFilter } = req.body;
        
        if (!apiData || !bqTable || !primaryKey) {
            return res.status(400).json({ 
                success: false, 
                error: 'apiData, bqTable, and primaryKey are required' 
            });
        }
        
        console.log(`\n=== API vs BQ COMPARISON ===`);
        console.log(`=Æ’Ã´Ã¨ API Records: ${apiData.length}`);
        console.log(`=Æ’Ã„Â» Target BQ Table: ${bqTable}`);
        console.log(`=Æ’Ã¶Ã¦ Primary Key: ${primaryKey}`);
        console.log(`=Æ’Ã¶Ã¬ BQ Filter: ${bqFilter || '(none)'}`);
        
        // Create temp table from API data
        const bqService = new BigQueryIntegrationService();
        const expandedApiData = Array.isArray(apiData) ? apiData : expandUUIDKeyedData(apiData);
        const tempTableResult = await bqService.createTempTableFromJSON(
            expandedApiData,
            `api_${Date.now()}`,
            primaryKey
        );
        
        console.log(`GÂ£Ã  Created temp table: ${tempTableResult.tempTableId}`);
        
        // Use comparison engine (same as JSON vs BQ)
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        let results;
        if (bqFilter) {
            // Use filtered comparison if filter is provided
            console.log(`=Æ’Ã¶Ã¬ Using filtered comparison with: ${bqFilter}`);
            results = await comparisonEngine.compareJSONvsBigQueryWithFilter(
                tempTableResult.tempTableId,
                bqTable,
                primaryKey,
                [],
                'enhanced',
                bqFilter,
                null
            );
        } else {
            // Use standard comparison without filter
            results = await comparisonEngine.compareJSONvsBigQuery(
                tempTableResult.tempTableId,
                bqTable,
                primaryKey,
                [],
                'enhanced'
            );
        }
        
        // Add metadata
        results.metadata = {
            ...results.metadata,
            sourceType: 'API',
            tempTable: tempTableResult.tempTableId,
            recordsProcessed: apiData.length,
            bqFilter: bqFilter || null
        };
        
        console.log(`GÂ£Ã  Comparison complete for ${bqTable}`);
        res.json(results);
        
    } catch (error) {
        console.error('GÂ¥Ã® API vs BQ comparison failed:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});


// ==================== END API vs BQ ====================

// Start Server
app.listen(port, () => {
    console.log(`\nðŸš€ ETL Data Validation Server started`);
    console.log(`ðŸ“ Port: ${port}`);
    console.log(`ðŸŒ URL: http://localhost:${port}`);
    console.log(`âœ… Oracle Thick Mode: Initialized`);
    console.log(`ðŸ“Š BigQuery: Ready`);
    console.log(`\nðŸ“‹ Available Endpoints:`);
    console.log(`   - GET  /api/health`);
    console.log(`   - POST /api/test-rdbms-connection`);
    console.log(`   - POST /api/rdbms-vs-bq`);
    console.log(`   - POST /api/bq-vs-bq`);
    console.log(`\n`);
});
