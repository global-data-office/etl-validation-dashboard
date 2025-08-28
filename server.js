// server.js - FIXED: Dynamic Table and Primary Key Support for ANY data type with CONSISTENT JSON parsing
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const jsonUploadRouter = require('./routes/json-upload');
const BigQueryIntegrationService = require('./services/bq-integration');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

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
            fixes: result.fixes || ['Dynamic table support', 'Dynamic primary key support', 'Batch processing for large files']
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

// FIXED: JSON vs BigQuery Comparison - Now with CONSISTENT JSON parsing
app.post('/api/compare-json-vs-bq', async (req, res) => {
    try {
        const { 
            fileId, 
            sourceTable,  // USER-SPECIFIED BigQuery table
            primaryKey,   // USER-SPECIFIED primary key
            comparisonFields = [],
            strategy = 'full' 
        } = req.body;
        
        console.log(`FIXED: Starting DYNAMIC comparison for file: ${fileId}`);
        console.log(`User-specified BigQuery table: ${sourceTable}`);
        console.log(`User-specified primary key: ${primaryKey}`);
        
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
                    'Enter a field name that exists in both JSON and BigQuery tables',
                    'For monitor data, try: id, account_id',
                    'For AWS data, try: Id, Arn, Catalog',
                    'For ServiceNow data, try: task_sys_id, task_number',
                    'You can use ANY field name that exists in both tables'
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
        
        console.log(`FIXED: Comparing using actual table: ${actualTempTableId} vs ${sourceTable}`);
        
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
        
        // Run dynamic comparison with user-specified parameters
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        console.log(`FIXED: Running dynamic comparison...`);
        console.log(`Using user's BigQuery table: ${sourceTable}`);
        console.log(`Using user's primary key: ${primaryKey}`);
        
        const results = await comparisonEngine.compareJSONvsBigQuery(
            actualTempTableId, // Use actual table ID
            sourceTable,      // Use user-specified table
            primaryKey,       // Use user-specified primary key
            comparisonFields,
            strategy
        );
        
        console.log(`Dynamic comparison completed successfully`);
        console.log(`Results summary: ${results.summary?.recordsReachedTarget || 0} matches found using '${primaryKey}'`);
        
        // Include temp table info in response
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
        
        res.json(results);
        
    } catch (error) {
        console.error('Dynamic comparison API failed:', error.message);
        
        // Enhanced error handling for schema issues
        let errorMessage = error.message;
        let suggestions = [
            'Check that the primary key field exists in both JSON and BigQuery tables',
            'Try using a different field that exists in both tables',
            'Verify BigQuery table is accessible'
        ];
        
        if (error.message.includes('not available in both tables')) {
            suggestions = [
                'Choose a field that exists in both your JSON file and BigQuery table',
                'For monitor data, try: id, account_id',
                'For AWS data, try: Id, Arn, Catalog',
                'For ServiceNow data, try: task_sys_id, task_number',
                'Check the Column Names tab after upload to see available common fields'
            ];
        } else if (error.message.includes('Unrecognized name')) {
            suggestions = [
                'The selected field does not exist in one of the tables',
                'Use the Column Names tab to see which fields are available in both tables',
                'Try a different primary key field',
                'Make sure field names match exactly (case-sensitive)'
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
            details: 'Dynamic comparison failed',
            suggestions: suggestions,
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

// Original Table Validation (v1.0 functionality - preserved unchanged)
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
                    message: 'Table name is required to run validation.',
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

        console.log('Running validation for table:', tableName);

        const query = `
            CALL \`${process.env.GOOGLE_CLOUD_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.data_validation_checks\`(
                @table_name,
                @null_check_columns,
                @duplicate_key_columns,
                @special_char_check_columns,
                @composite_key_columns
            )
        `;

        const options = {
            query: query,
            params: {
                table_name: tableName,
                null_check_columns: nullCheckColumns || [],
                duplicate_key_columns: duplicateKeyColumns || [],
                special_char_check_columns: specialCharCheckColumns || [],
                composite_key_columns: compositeKeyColumns || []
            },
            types: {
                table_name: 'STRING',
                null_check_columns: ['STRING'],
                duplicate_key_columns: ['STRING'],
                special_char_check_columns: ['STRING'],
                composite_key_columns: ['STRING']
            }
        };

        const [rows] = await bigquery.query(options);
        console.log('Validation results:', rows);

        res.json({
            success: true,
            data: rows,
            timestamp: new Date().toISOString(),
            table: tableName
        });

    } catch (error) {
        console.error('Error running validation:', error);

        let errorResponse = {
            success: false,
            error: {
                type: 'UNKNOWN_ERROR',
                title: 'Validation Error',
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

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({
        status: 'OK',
        version: 'v2.1-CONSISTENT-PARSING',
        timestamp: new Date().toISOString(),
        bigqueryProject: process.env.GOOGLE_CLOUD_PROJECT_ID,
        features: {
            dynamicTableSupport: true,
            dynamicPrimaryKeySupport: true,
            batchProcessingForLargeFiles: true,
            anyDataTypeSupport: true,
            zeroRecordDuplication: true,
            enhancedPreviewEndpoint: true,
            consistentJSONParsing: true,
            supportedDataTypes: ['ServiceNow', 'AWS Partner Central', 'Monitor Details', 'Pool Details', 'Any JSON/JSONL'],
            maxFileSize: '100MB',
            batchSize: '1000 records per batch'
        }
    });
});

// Serve the dashboard HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(port, '0.0.0.0', () => {
    console.log(`ETL Validation Dashboard v2.1-CONSISTENT-PARSING server running on port ${port}`);
    console.log(`Dashboard available at: http://localhost:${port}`);
    console.log(`BigQuery Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
    console.log(`FIXES APPLIED:`);
    console.log(`  - Dynamic table support (any BigQuery table)`);
    console.log(`  - Dynamic primary key support (any field name)`);
    console.log(`  - Batch processing for large files (up to 100MB)`);
    console.log(`  - Support for ANY data type (ServiceNow, AWS, Monitor, Pool Details, etc.)`);
    console.log(`  - CONSISTENT: Unified JSON parsing across all endpoints`);
    console.log(`  - ENHANCED: Comprehensive field analysis and preview`);
    console.log(`  - ENHANCED: Detailed logging for troubleshooting`);
});