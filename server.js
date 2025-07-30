// server.js - Backend server for BigQuery ETL Validation Dashboard

const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const jsonUploadRouter = require('./routes/json-upload');
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