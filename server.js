// server.js - ETL Dashboard v2.1: Custom primary keys + ALL fields + BigQuery duplicates
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

// Schema Analysis Endpoint
app.post('/api/analyze-schemas', async (req, res) => {
    try {
        const { tempTableId, sourceTable } = req.body;
        
        if (!tempTableId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId and sourceTable are required'
            });
        }
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const schemaAnalysis = await comparisonEngine.getCommonFields(tempTableId, sourceTable);
        
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

// Get ALL Fields from JSON File for Custom Primary Key Selection
app.get('/api/get-json-fields/:fileId', async (req, res) => {
    try {
        const { fileId } = req.params;
        
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
                error: 'File not found for field extraction'
            });
        }
        
        // Parse and get ALL fields from ALL records
        const fileContent = fs.readFileSync(filePath, 'utf8');
        let jsonData = [];
        
        if (filePath.endsWith('.jsonl') || fileContent.includes('\n{')) {
            const lines = fileContent.trim().split('\n');
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        jsonData.push(JSON.parse(line.trim()));
                    } catch (parseError) {
                        console.warn('Skipping invalid JSON line');
                    }
                }
            }
        } else {
            const parsed = JSON.parse(fileContent);
            jsonData = Array.isArray(parsed) ? parsed : [parsed];
        }
        
        // Extract ALL fields from ALL records
        const allFields = new Set();
        const fieldFrequency = {};
        
        jsonData.forEach((record, index) => {
            function extractFields(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const fieldName = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        if (value.display_value !== undefined) {
                            allFields.add(`${fieldName}_display_value`);
                            fieldFrequency[`${fieldName}_display_value`] = (fieldFrequency[`${fieldName}_display_value`] || 0) + 1;
                        }
                        if (value.link !== undefined) {
                            allFields.add(`${fieldName}_link`);
                            fieldFrequency[`${fieldName}_link`] = (fieldFrequency[`${fieldName}_link`] || 0) + 1;
                        }
                        if (value.value !== undefined) {
                            allFields.add(`${fieldName}_value`);
                            fieldFrequency[`${fieldName}_value`] = (fieldFrequency[`${fieldName}_value`] || 0) + 1;
                        }
                        
                        if (!value.display_value && !value.link && !value.value && prefix.split('_').length < 2) {
                            extractFields(value, fieldName);
                        }
                    } else {
                        allFields.add(fieldName);
                        fieldFrequency[fieldName] = (fieldFrequency[fieldName] || 0) + 1;
                    }
                }
            }
            
            extractFields(record);
        });
        
        const detectedFields = Array.from(allFields).sort();
        
        // Categorize fields for better primary key suggestions
        const idFields = detectedFields.filter(field => {
            const lowerField = field.toLowerCase();
            return lowerField.includes('id') ||
                   lowerField.includes('key') ||
                   lowerField.includes('number') ||
                   lowerField.includes('_id') ||
                   lowerField.includes('sys_') ||
                   ['sys_id', 'number', 'id', 'key'].includes(lowerField);
        });
        
        const importantFields = detectedFields.filter(field => {
            const lowerField = field.toLowerCase();
            return !idFields.includes(field) && (
                lowerField.includes('name') ||
                lowerField.includes('code') ||
                lowerField.includes('type') ||
                lowerField.includes('status') ||
                lowerField.includes('priority') ||
                lowerField.includes('state')
            );
        });
        
        const otherFields = detectedFields.filter(field => 
            !idFields.includes(field) && !importantFields.includes(field)
        );
        
        res.json({
            success: true,
            message: 'All fields extracted successfully from all records',
            totalFields: detectedFields.length,
            allFields: detectedFields,
            categorizedFields: {
                idFields: idFields,
                importantFields: importantFields,
                otherFields: otherFields
            },
            fieldFrequency: fieldFrequency,
            primaryKeyCandidates: idFields
        });
        
    } catch (error) {
        console.error('Field extraction failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Field extraction failed'
        });
    }
});

// Validate Custom Primary Key Field
app.post('/api/validate-primary-key', async (req, res) => {
    try {
        const { tempTableId, sourceTable, primaryKey } = req.body;
        
        if (!tempTableId || !sourceTable || !primaryKey) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId, sourceTable, and primaryKey are required'
            });
        }
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        // Get common fields
        const schemaAnalysis = await comparisonEngine.getCommonFields(tempTableId, sourceTable);
        
        // Validate the specific primary key
        if (!schemaAnalysis.commonFields.includes(primaryKey)) {
            return res.status(400).json({
                success: false,
                error: `Primary key '${primaryKey}' not found in both tables`,
                availableFields: schemaAnalysis.commonFields,
                suggestions: schemaAnalysis.primaryKeyCandidates,
                details: `Available common fields: ${schemaAnalysis.commonFields.slice(0, 10).join(', ')}`
            });
        }
        
        const keyValidation = await comparisonEngine.validatePrimaryKeyField(
            tempTableId, 
            sourceTable, 
            primaryKey, 
            schemaAnalysis.commonFields
        );
        
        res.json({
            success: true,
            message: `Primary key '${primaryKey}' is valid for comparison`,
            primaryKey: primaryKey,
            validation: keyValidation,
            commonFields: schemaAnalysis.commonFields
        });
        
    } catch (error) {
        console.error('Primary key validation failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Primary key validation failed',
            suggestions: [
                'Check that the field exists in both tables',
                'Try a different field name',
                'Use common fields like: task_sys_id, task_number, sys_id'
            ]
        });
    }
});

// BigQuery Connection Test
app.get('/api/test-bq-connection', async (req, res) => {
    try {
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

// Test Source Table Access
app.get('/api/test-source-table', async (req, res) => {
    try {
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.testSourceTableAccess();
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

// Create Temp Table with ALL Fields Support
app.post('/api/create-temp-table', async (req, res) => {
    try {
        const { fileId } = req.body;
        
        if (!fileId) {
            return res.status(400).json({
                success: false,
                error: 'File ID is required'
            });
        }
        
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
                error: 'File not found',
                details: `File ${fileId} not found in any expected location`
            });
        }
        
        // Read and parse the JSON file
        const fileContent = fs.readFileSync(filePath, 'utf8');
        let jsonData = [];
        
        // Handle JSONL format
        if (filePath.endsWith('.jsonl') || fileContent.includes('\n{')) {
            const lines = fileContent.trim().split('\n');
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        const record = JSON.parse(line.trim());
                        jsonData.push(record);
                    } catch (parseError) {
                        console.warn('Skipping invalid JSON line:', line.substring(0, 50));
                    }
                }
            }
        } else {
            try {
                const parsed = JSON.parse(fileContent);
                jsonData = Array.isArray(parsed) ? parsed : [parsed];
            } catch (parseError) {
                console.error('JSON parsing failed:', parseError.message);
                return res.status(400).json({
                    success: false,
                    error: 'Invalid JSON format',
                    details: parseError.message
                });
            }
        }
        
        if (jsonData.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No valid JSON data found in file'
            });
        }
        
        // Flatten nested objects for BigQuery compatibility
        const flattenedData = jsonData.map((record, index) => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        // Handle ServiceNow reference objects
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
                            // Regular nested object - flatten with depth limit
                            if (prefix.split('_').length < 3) {
                                flattenObject(value, newKey);
                            } else {
                                flattened[newKey] = JSON.stringify(value);
                            }
                        }
                    } else if (Array.isArray(value)) {
                        flattened[newKey] = JSON.stringify(value);
                    } else {
                        flattened[newKey] = value === null || value === undefined ? null : String(value);
                    }
                }
            }
            
            flattenObject(record);
            return flattened;
        });
        
        // Create temp table
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(flattenedData, fileId);
        
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
            verification: result.verification,
            expiresAt: result.expiresAt
        });
        
    } catch (error) {
        console.error('Temp table creation failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Temp table creation failed'
        });
    }
});

// Comprehensive JSON vs BigQuery Comparison (All 3 fixes applied)
app.post('/api/compare-json-vs-bq', async (req, res) => {
    try {
        const { 
            fileId, 
            sourceTable, 
            primaryKey = 'task_sys_id',
            comparisonFields = [],
            strategy = 'enhanced-all-fixes' 
        } = req.body;
        
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
                    'Common options: task_sys_id, task_number, sys_id, number',
                    'You can use ANY field name that exists in both tables'
                ]
            });
        }
        
        // Process JSON data
        const fs = require('fs');
        const path = require('path');
        
        const possiblePaths = [
            path.join(__dirname, 'uploads', `${fileId}.json`),
            path.join(__dirname, 'uploads', `${fileId}.jsonl`),
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
        
        // Parse the JSON data
        const fileContent = fs.readFileSync(filePath, 'utf8');
        let jsonData = [];
        
        if (filePath.endsWith('.jsonl') || fileContent.includes('\n{')) {
            const lines = fileContent.trim().split('\n');
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        jsonData.push(JSON.parse(line.trim()));
                    } catch (parseError) {
                        console.warn('Skipping invalid JSON line');
                    }
                }
            }
        } else {
            const parsed = JSON.parse(fileContent);
            jsonData = Array.isArray(parsed) ? parsed : [parsed];
        }
        
        // Flatten the data
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
                        flattened[newKey] = value === null || value === undefined ? null : String(value);
                    }
                }
            }
            
            flattenObject(record);
            return flattened;
        });
        
        // Create temp table and get actual table ID
        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(flattenedData, fileId);
        
        const actualTempTableId = tempTableResult.tempTableId;
        
        // Pre-comparison verification
        try {
            const [preCheckResult] = await bigquery.query(`SELECT COUNT(*) as count FROM \`${actualTempTableId}\``);
            const tempTableCount = preCheckResult[0].count;
            
            if (tempTableCount === 0) {
                return res.status(400).json({
                    success: false,
                    error: 'Temp table is empty',
                    details: `No records found in temp table: ${actualTempTableId}`
                });
            }
        } catch (preCheckError) {
            return res.status(400).json({
                success: false,
                error: 'Cannot access temp table',
                details: preCheckError.message
            });
        }
        
        // Run comprehensive comparison
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const results = await comparisonEngine.compareJSONvsBigQuery(
            actualTempTableId,
            sourceTable,
            primaryKey,
            comparisonFields,
            strategy
        );
        
        // Include temp table info in response
        results.tempTableInfo = {
            actualTableId: actualTempTableId,
            recordsInTable: tempTableResult.recordsInTable,
            recordCountMatch: tempTableResult.recordCountMatch
        };
        
        res.json(results);
        
    } catch (error) {
        console.error('Comparison API failed:', error.message);
        
        // Better error handling for custom primary key issues
        let errorMessage = error.message;
        let suggestions = [
            'Check that the primary key field exists in both JSON and BigQuery tables',
            'Try using a different field that exists in both tables',
            'Verify BigQuery table is accessible',
            'You can use ANY field name that exists in both tables'
        ];
        
        if (error.message.includes('not available in both tables')) {
            suggestions = [
                'The field you entered does not exist in both tables',
                'Upload your file again to see available common fields in Column Names tab',
                'Try common fields like: task_sys_id, task_number, task_priority, sys_id',
                'Remember: You can type ANY field name that exists in both tables'
            ];
        } else if (error.message.includes('Unrecognized name')) {
            suggestions = [
                'The selected field does not exist in one of the tables',
                'Check spelling and make sure field exists in both JSON and BigQuery',
                'Try a different primary key field',
                'Use the Column Names tab to see available common fields'
            ];
        }
        
        res.status(500).json({
            success: false,
            error: errorMessage,
            details: 'Comparison failed',
            suggestions: suggestions,
            timestamp: new Date().toISOString()
        });
    }
});

// Detailed Duplicates Analysis for Both Tables
app.post('/api/analyze-duplicates-detailed', async (req, res) => {
    try {
        const { tempTableId, sourceTable, primaryKey } = req.body;
        
        if (!tempTableId || !sourceTable || !primaryKey) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId, sourceTable, and primaryKey are required'
            });
        }
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        // Run duplicates analysis for BOTH tables
        const duplicatesAnalysis = await comparisonEngine.analyzeDuplicatesEnhanced(tempTableId, sourceTable, primaryKey);
        
        res.json({
            success: true,
            message: 'Duplicates analysis completed for both tables',
            duplicatesAnalysis: duplicatesAnalysis
        });
        
    } catch (error) {
        console.error('Duplicates analysis failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Duplicates analysis failed'
        });
    }
});

// Get Common Fields for Custom Primary Key Validation
app.post('/api/get-common-fields', async (req, res) => {
    try {
        const { tempTableId, sourceTable } = req.body;
        
        if (!tempTableId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId and sourceTable are required'
            });
        }
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const schemaAnalysis = await comparisonEngine.getCommonFields(tempTableId, sourceTable);
        
        res.json({
            success: true,
            message: 'Common fields analysis completed',
            commonFields: schemaAnalysis.commonFields,
            primaryKeyCandidates: schemaAnalysis.primaryKeyCandidates,
            totalJsonFields: schemaAnalysis.totalJsonFields,
            totalBqFields: schemaAnalysis.totalBqFields,
            schemaCompatibility: schemaAnalysis.schemaCompatibility
        });
        
    } catch (error) {
        console.error('Common fields analysis failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Common fields analysis failed'
        });
    }
});

// Original Table Validation (v1.0 functionality - preserved)
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
        version: 'v2.1',
        timestamp: new Date().toISOString(),
        bigqueryProject: process.env.GOOGLE_CLOUD_PROJECT_ID,
        features: {
            customPrimaryKeySupport: true,
            allFieldsAnalysis: true,
            bigQueryDuplicateDetection: true,
            zeroRecordDuplication: true
        }
    });
});

// Serve the dashboard HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Additional diagnostic endpoints
app.get('/api/get-source-schema', async (req, res) => {
    try {
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.getSourceTableSchema();
        res.json(result);
    } catch (error) {
        console.error('Schema retrieval failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Schema retrieval failed'
        });
    }
});

app.get('/api/get-sample-data', async (req, res) => {
    try {
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.getSampleData();
        res.json(result);
    } catch (error) {
        console.error('Sample data retrieval failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Sample data retrieval failed'
        });
    }
});

// Manual cleanup endpoint
app.delete('/api/cleanup-temp-table/:fileId', async (req, res) => {
    try {
        const { fileId } = req.params;
        
        const dataset = bigquery.dataset('temp_validation_tables');
        
        let deletedTables = [];
        let errors = [];
        
        try {
            const [tables] = await dataset.getTables();
            const relevantTables = tables.filter(table => 
                table.id.startsWith(`json_temp_${fileId}`)
            );
            
            for (const table of relevantTables) {
                try {
                    await table.delete();
                    deletedTables.push(table.id);
                } catch (deleteError) {
                    errors.push(`Failed to delete ${table.id}: ${deleteError.message}`);
                }
            }
            
        } catch (listError) {
            errors.push(`Failed to list tables: ${listError.message}`);
        }
        
        res.json({
            success: deletedTables.length > 0 || errors.length === 0,
            deletedTables: deletedTables,
            errors: errors,
            message: `Cleanup completed: ${deletedTables.length} tables deleted, ${errors.length} errors`
        });
        
    } catch (error) {
        console.error('Cleanup failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Start server
app.listen(port, '0.0.0.0', () => {
    console.log(`🚀 ETL Validation Dashboard v2.1 server running on port ${port}`);
    console.log(`📊 Dashboard available at: http://localhost:${port}`);
    console.log(`☁️ BigQuery Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
    console.log(`🎯 Features: Custom primary keys + ALL fields analysis + BigQuery duplicates`);
});