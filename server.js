// server.js - Backend server for BigQuery ETL Validation Dashboard

const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const jsonUploadRouter = require('./routes/json-upload');
const BigQueryIntegrationService = require('./services/bq-integration'); // NEW: Add BQ Integration Service
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

// NEW: BigQuery Connection Test Endpoint
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

// NEW: Test Source Table Access
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

// NEW: Get Source Table Schema
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

// NEW: Get Sample Data
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

// NEW: Create Temp Table from JSON (FIXED VERSION)
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
        
        // Direct file reading approach
        const fs = require('fs');
        const path = require('path');
        
        // Try different possible file paths
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
            console.log('🔍 Checked paths:', possiblePaths);
            
            // List actual files in uploads directory
            const uploadsDir = path.join(__dirname, 'uploads');
            if (fs.existsSync(uploadsDir)) {
                const files = fs.readdirSync(uploadsDir);
                console.log('📂 Files in uploads directory:', files);
            }
            
            return res.status(404).json({
                success: false,
                error: 'File not found',
                details: `File ${fileId} not found in any expected location`,
                checkedPaths: possiblePaths
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
            // Handle regular JSON array format
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
        
        // Flatten nested objects for BigQuery compatibility
        const flattenedData = jsonData.map(record => {
            const flattened = {};
            
            function flattenObject(obj, prefix = '') {
                for (const [key, value] of Object.entries(obj)) {
                    const newKey = prefix ? `${prefix}_${key}` : key;
                    
                    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                        // Handle nested objects (like ServiceNow references)
                        if (value.link && value.value) {
                            // ServiceNow reference object
                            flattened[`${newKey}_link`] = value.link;
                            flattened[`${newKey}_value`] = value.value;
                        } else {
                            // Regular nested object - flatten recursively
                            flattenObject(value, newKey);
                        }
                    } else if (Array.isArray(value)) {
                        // Convert arrays to JSON strings
                        flattened[newKey] = JSON.stringify(value);
                    } else {
                        // Regular field
                        flattened[newKey] = value;
                    }
                }
            }
            
            flattenObject(record);
            return flattened;
        });
        
        console.log(`🔧 Flattened data with ${Object.keys(flattenedData[0]).length} fields`);
        
        // Create temp table
        const bqService = new BigQueryIntegrationService();
        const result = await bqService.createTempTableFromJSON(flattenedData, fileId);
        
        res.json(result);
        
    } catch (error) {
        console.error('❌ Temp table creation failed:', error.message);
        console.error('❌ Full error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'Failed to create temp table from JSON'
        });
    }
});

// NEW: JSON vs BigQuery Comparison
app.post('/api/compare-json-vs-bq', async (req, res) => {
    try {
        const { 
            fileId, 
            sourceTable, 
            primaryKey = 'sys_id', 
            comparisonFields = [],
            strategy = 'full' 
        } = req.body;
        
        console.log(`🚀 Starting comparison for file: ${fileId}`);
        
        if (!fileId || !sourceTable) {
            return res.status(400).json({
                success: false,
                error: 'fileId and sourceTable are required'
            });
        }
        
        // Build temp table ID from file ID
        const tempTableName = `json_temp_${fileId}`;
        const tempTableId = `${process.env.GOOGLE_CLOUD_PROJECT_ID}.temp_validation_tables.${tempTableName}`;
        
        console.log(`📊 Comparing: ${tempTableId} vs ${sourceTable}`);
        
        // Create comparison engine and run comparison
        const ComparisonEngineService = require('./services/comparison-engine');
        const comparisonEngine = new ComparisonEngineService();
        
        const results = await comparisonEngine.compareJSONvsBigQuery(
            tempTableId,
            sourceTable,
            primaryKey,
            comparisonFields
        );
        
        console.log(`✅ Comparison completed: ${results.summary.matchingRecords} matches found`);
        
        res.json(results);
        
    } catch (error) {
        console.error('❌ Comparison API failed:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            details: 'JSON vs BigQuery comparison failed'
        });
    }
});

// API endpoint to run validation
app.post('/api/validate', async (req, res) => {
    try {
        const {
            tableName,
            nullCheckColumns,
            duplicateKeyColumns,
            specialCharCheckColumns,
            compositeKeyColumns
        } = req.body;

        // Enhanced input validation
        if (!tableName) {
            return res.status(400).json({
                success: false,
                error: {
                    type: 'MISSING_TABLE_NAME',
                    title: 'Missing Table Name',
                    message: 'Table name is required to run validation.',
                    details: 'No table name provided in request',
                    suggestions: [
                        'Enter a valid BigQuery table name',
                        'Use format: project.dataset.table'
                    ]
                }
            });
        }

        // Validate table name format
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

        // Construct the SQL query to call your stored procedure
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

        // Execute the query
        const [rows] = await bigquery.query(options);
        console.log('Validation results:', rows);

        // Return results
        res.json({
            success: true,
            data: rows,
            timestamp: new Date().toISOString(),
            table: tableName
        });

    } catch (error) {
        console.error('Error running validation:', error);

        // Enhanced error handling with specific messages
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
                    'Ensure table format is correct',
                    'Contact support if the issue persists'
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
                    'Ensure you have proper permissions to access the table',
                    'Check if the project and dataset names are correct'
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
                    'Use BigQuery console to view table schema',
                    'Remove any extra spaces or special characters'
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
                    'Verify your service account has proper roles',
                    'Check if you have access to the specified project/dataset'
                ]
            };
        } else if (error.message.includes('Syntax error') || error.message.includes('Invalid')) {
            errorResponse.error = {
                type: 'SYNTAX_ERROR',
                title: 'Invalid Input Format',
                message: 'There is a syntax error in your input parameters.',
                details: error.message,
                suggestions: [
                    'Check table name format: project.dataset.table',
                    'Ensure column names are comma-separated',
                    'Remove any special characters except underscores and hyphens',
                    'Check for extra commas or spaces'
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
});