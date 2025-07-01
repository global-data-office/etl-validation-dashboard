// server.js - Backend server for BigQuery ETL Validation Dashboard
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public')); // Serve static files

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
      specialCharCheckColumns 
    } = req.body;

    // Validate input
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    console.log('Running validation for table:', tableName);

    // Construct the SQL query to call your stored procedure
    const query = `
      CALL \`${process.env.GOOGLE_CLOUD_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.data_validation_checks\`(
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
    res.status(500).json({ 
      error: 'Failed to run validation', 
      message: error.message
    });
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
  console.log(`☁️  BigQuery Project: ${process.env.GOOGLE_CLOUD_PROJECT_ID}`);
});