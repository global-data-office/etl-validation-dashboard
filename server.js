// server.js - FINAL UPDATED VERSION: Schema-Safe Any Column Comparison
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

// NEW: Schema Analysis Endpoint for Frontend
app.post('/api/analyze-schemas', async (req, res) => {
    try {
        const { tempTableId, sourceTable } = req.body;
        
        if (!tempTableId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'tempTableId and sourceTable are required'
            });
        }
        
        console.log('📋 Analyzing schemas for comprehensive comparison...');
        
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const schemaAnalysis = await comparisonEngine.getCommonFields(tempTableId, sourceTable);
        
        console.log(`✅ Schema analysis complete: ${schemaAnalysis.commonFields.length} common fields found`);
        
        res.json({
            success: true,
            ...schemaAnalysis
        });
        
    } catch (error) {
        console.error('❌ Schema analysis failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Schema analysis failed'
        });
    }
});

// BigQuery Connection Test Endpoint
app.get('/api/test-bq-connection', async (req, res) => {
    try {
        console.log('🧪 Testing BigQuery connection via API...');
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.testConnection();
        res.json(result);
    } catch (error) {
        console.error('❌ BigQuery connection test failed:', error.message);
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
        console.log('🔍 Testing source table access via API...');
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.testSourceTableAccess();
        res.json(result);
    } catch (error) {
        console.error('❌ Source table test failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Source table access test failed'
        });
    }
});

// Get Source Table Schema
app.get('/api/get-source-schema', async (req, res) => {
    try {
        console.log('📋 Getting source table schema via API...');
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.getSourceTableSchema();
        res.json(result);
    } catch (error) {
        console.error('❌ Schema retrieval failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Schema retrieval failed'
        });
    }
});

// Get Sample Data
app.get('/api/get-sample-data', async (req, res) => {
    try {
        console.log('📊 Getting sample data via API...');
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.getSampleData();
        res.json(result);
    } catch (error) {
        console.error('❌ Sample data retrieval failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Sample data retrieval failed'
        });
    }
});

// Create Temp Table from JSON (Returns Actual Table ID)
app.post('/api/create-temp-table', async (req, res) => {
    try {
        const { fileId } = req.body;
        
        if (!fileId) {
            return res.status(400).json({
                success: false,
                error: 'File ID is required'
            });
        }
        
        console.log(`🔧 Creating temp table for file: ${fileId}`);
        
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
                console.log(`📁 Found file at: ${filePath}`);
                break;
            }
        }
        
        if (!filePath) {
            console.log('❌ File not found in any expected location');
            return res.status(404).json({
                success: false,
                error: 'File not found',
                details: `File ${fileId} not found in any expected location`
            });
        }
        
        // Read and parse the JSON file
        console.log(`📖 Reading file: ${filePath}`);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        
        let jsonData = [];
        
        // Handle JSONL format (line-delimited JSON)
        if (filePath.endsWith('.jsonl') || fileContent.includes('\n{')) {
            console.log('📋 Processing JSONL format...');
            const lines = fileContent.trim().split('\n');
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        const record = JSON.parse(line.trim());
                        jsonData.push(record);
                    } catch (parseError) {
                        console.warn('⚠️ Skipping invalid JSON line:', line.substring(0, 50));
                    }
                }
            }
        } else {
            console.log('📋 Processing JSON array format...');
            try {
                const parsed = JSON.parse(fileContent);
                jsonData = Array.isArray(parsed) ? parsed : [parsed];
            } catch (parseError) {
                console.error('❌ JSON parsing failed:', parseError.message);
                return res.status(400).json({
                    success: false,
                    error: 'Invalid JSON format',
                    details: parseError.message
                });
            }
        }
        
        console.log(`✅ Parsed ${jsonData.length} records from file`);
        
        if (jsonData.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No valid JSON data found in file'
            });
        }
        
        // Check for primary key fields
        console.log('🔍 Checking for key fields in first record...');
        const firstRecord = jsonData[0];
        console.log('🔧 First record keys (first 10):', Object.keys(firstRecord).slice(0, 10));
        console.log('🔑 task_sys_id value:', firstRecord.task_sys_id);
        
        // Flatten nested objects for BigQuery compatibility
        const flattenedData = jsonData.map((record, index) => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        // Handle nested objects (like ServiceNow references)
                        if (value.display_value || value.link) {
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
                            flattenObject(value, newKey);
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
            
            if (index === 0) {
                console.log('🔧 After flattening, task_sys_id value:', flattened.task_sys_id);
                console.log('🔧 Flattened keys sample:', Object.keys(flattened).slice(0, 20));
            }
            
            return flattened;
        });
        
        console.log(`🔧 Flattened data with ${Object.keys(flattenedData[0]).length} fields`);
        
        // Final verification before sending to BigQuery
        console.log('🔍 Final verification - sample task_sys_id values:');
        flattenedData.slice(0, Math.min(3, flattenedData.length)).forEach((record, i) => {
            console.log(`Record ${i}: task_sys_id = ${record.task_sys_id}`);
        });
        
        // Create temp table
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(flattenedData, fileId);
        
        console.log('🔍 Final verification after temp table creation...');
        console.log(`📊 Records in table: ${result.recordsInTable}`);
        console.log(`📋 Records input: ${result.inputRecords}`);
        console.log(`🎯 Actual temp table ID: ${result.tempTableId}`);
        
        res.json({
            success: true,
            message: result.message,
            tempTableId: result.tempTableId, // Return actual table ID
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
        console.error('❌ Temp table creation failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Failed to create temp table from JSON'
        });
    }
});

// FINAL: Schema-Safe JSON vs BigQuery Comparison
app.post('/api/compare-json-vs-bq', async (req, res) => {
    try {
        const { 
            fileId, 
            sourceTable, 
            primaryKey = 'task_sys_id', 
            comparisonFields = [],
            strategy = 'full' 
        } = req.body;
        
        console.log(`🚀 FINAL: Starting schema-safe comparison for file: ${fileId}`);
        console.log(`🔑 Primary key: ${primaryKey}`);
        console.log(`🔧 Comparison fields: ${comparisonFields.length > 0 ? comparisonFields.join(', ') : 'Auto-selected common fields'}`);
        
        if (!fileId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'fileId and sourceTable are required'
            });
        }
        
        // Step 1: Create temp table and get actual table ID
        console.log(`🔍 STEP 1: Creating temp table and getting actual table ID...`);
        
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
        
        // Parse the JSON data (same as create-temp-table)
        const fileContent = fs.readFileSync(filePath, 'utf8');
        let jsonData = [];
        
        if (filePath.endsWith('.jsonl') || fileContent.includes('\n{')) {
            const lines = fileContent.trim().split('\n');
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        jsonData.push(JSON.parse(line.trim()));
                    } catch (parseError) {
                        console.warn('⚠️ Skipping invalid JSON line');
                    }
                }
            }
        } else {
            const parsed = JSON.parse(fileContent);
            jsonData = Array.isArray(parsed) ? parsed : [parsed];
        }
        
        console.log(`✅ Re-parsed ${jsonData.length} records for comparison`);
        
        // Flatten the data (same as create-temp-table)
        const flattenedData = jsonData.map((record) => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        if (value.display_value || value.link) {
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
                            flattenObject(value, newKey);
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
        
        // Create temp table and get actual table ID
        const bqService = new BigQueryIntegrationService();
        const tempTableResult = await bqService.createTempTableFromJSON(flattenedData, fileId);
        
        console.log(`✅ Temp table created successfully`);
        console.log(`🎯 ACTUAL temp table ID: ${tempTableResult.tempTableId}`);
        
        // Use the actual temp table ID returned from creation
        const actualTempTableId = tempTableResult.tempTableId;
        
        console.log(`📊 FINAL: Comparing using actual table: ${actualTempTableId} vs ${sourceTable}`);
        
        // Pre-comparison verification
        try {
            const [preCheckResult] = await bigquery.query(`SELECT COUNT(*) as count FROM \`${actualTempTableId}\``);
            const tempTableCount = preCheckResult[0].count;
            console.log(`🔍 Pre-comparison check: ${tempTableCount} records in temp table`);
            
            if (tempTableCount === 0) {
                console.error(`❌ CRITICAL: Temp table is empty!`);
                return res.status(400).json({
                    success: false,
                    error: 'Temp table is empty',
                    details: `No records found in temp table: ${actualTempTableId}`
                });
            }
        } catch (preCheckError) {
            console.error(`❌ Pre-comparison check failed:`, preCheckError.message);
            return res.status(400).json({
                success: false,
                error: 'Cannot access temp table',
                details: preCheckError.message
            });
        }
        
        // Run schema-safe comparison
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        console.log(`🔍 FINAL: Running schema-safe comparison...`);
        
        const results = await comparisonEngine.compareJSONvsBigQuery(
            actualTempTableId, // Use actual table ID
            sourceTable,
            primaryKey,
            comparisonFields,
            strategy
        );
        
        console.log(`✅ Schema-safe comparison completed successfully`);
        console.log(`📊 Results summary: ${results.summary?.recordsReachedTarget || 0} matches found using '${primaryKey}'`);
        
        // Include temp table info in response
        results.tempTableInfo = {
            actualTableId: actualTempTableId,
            recordsInTable: tempTableResult.recordsInTable,
            recordCountMatch: tempTableResult.recordCountMatch
        };
        
        res.json(results);
        
    } catch (error) {
        console.error('❌ Schema-safe comparison API failed:', error.message);
        
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
                'Common options: task_sys_id, task_number, task_priority',
                'Check the Column Names tab after upload to see available common fields'
            ];
        } else if (error.message.includes('Unrecognized name')) {
            suggestions = [
                'The selected field does not exist in one of the tables',
                'Use the Column Names tab to see which fields are available in both tables',
                'Try a different primary key field'
            ];
        }
        
        res.status(500).json({
            success: false,
            error: errorMessage,
            details: 'Schema-safe comparison failed',
            suggestions: suggestions,
            timestamp: new Date().toISOString()
        });
    }
});

// Manual Cleanup Endpoint
app.delete('/api/cleanup-temp-table/:fileId', async (req, res) => {
    try {
        const { fileId } = req.params;
        console.log(`🧹 Manual cleanup for file: ${fileId}`);
        
        const dataset = bigquery.dataset('temp_validation_tables');
        
        let deletedTables = [];
        let errors = [];
        
        try {
            const [tables] = await dataset.getTables();
            const relevantTables = tables.filter(table => 
                table.id.startsWith(`json_temp_${fileId}`)
            );
            
            console.log(`🔍 Found ${relevantTables.length} relevant tables to clean up`);
            
            for (const table of relevantTables) {
                try {
                    await table.delete();
                    deletedTables.push(table.id);
                    console.log(`✅ Deleted table: ${table.id}`);
                } catch (deleteError) {
                    errors.push(`Failed to delete ${table.id}: ${deleteError.message}`);
                    console.error(`❌ Failed to delete ${table.id}:`, deleteError.message);
                }
            }
            
        } catch (listError) {
            console.error(`❌ Failed to list tables:`, listError.message);
            errors.push(`Failed to list tables: ${listError.message}`);
        }
        
        res.json({
            success: deletedTables.length > 0 || errors.length === 0,
            deletedTables: deletedTables,
            errors: errors,
            message: `Cleanup completed: ${deletedTables.length} tables deleted, ${errors.length} errors`
        });
        
    } catch (error) {
        console.error('❌ Manual cleanup failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Original Table Validation (v1.0 functionality - unchanged)
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
        timestamp: new Date().toISOString(),
        bigqueryProject: process.env.GOOGLE_CLOUD_PROJECT_ID
    });
});

// Serve the dashboard HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(port, '0.0.0.0', () => {
    console.log(`🚀 ETL Validation Dashboard server running on port ${port}`);
    console.log(`📊 Dashboard available at: http://localhost:${port}`);
    console.log(`☁️ BigQuery Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
    console.log(`🔧 FEATURES: Schema-safe any-column comparison, no record duplication, 5-tab analysis`);
});