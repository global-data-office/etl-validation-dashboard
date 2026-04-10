# Requirements Document

## Introduction

This feature adds optional custom query support to the BQ vs BQ comparison tab in the ETL Data Validation Dashboard. Currently, the BQ vs BQ tab only supports table-based comparison (source dataset + table names mapped to target dataset + same table names). This feature introduces two optional textarea fields for source and target custom SQL queries, enabling users to compare arbitrary BigQuery query results against each other. When both custom queries are provided, the system bypasses table/dataset validation and instead executes the raw queries against BigQuery, comparing the results using the same field-by-field analysis pipeline.

## Glossary

- **Dashboard**: The ETL Data Validation Dashboard, a single-page HTML frontend served by a Node.js/Express backend
- **BQ_vs_BQ_Tab**: The existing BigQuery vs BigQuery comparison page in the Dashboard, identified by `bq-comparison-page`
- **Config_Form**: The configuration form within the BQ_vs_BQ_Tab containing source/target dataset inputs, table names, primary keys, and comparison fields
- **Custom_Query_Mode**: An optional operating mode activated when both source and target custom query textareas contain SQL text
- **Table_Mode**: The default operating mode where source dataset + table names are used to construct fully-qualified BigQuery table references
- **Source_Custom_Query_Field**: The textarea input (`bqvsbq-source-custom-query`) for entering an arbitrary BigQuery SQL query for source data
- **Target_Custom_Query_Field**: The textarea input (`bqvsbq-target-custom-query`) for entering an arbitrary BigQuery SQL query for target data
- **Custom_Query_Endpoint**: The new backend API endpoint `/api/bq-vs-bq-custom` that accepts two SQL queries and comparison parameters
- **Table_Endpoint**: The existing backend API endpoint `/api/bq-vs-bq` that accepts fully-qualified table names
- **Comparison_Engine**: The backend logic that performs record counts, sample matching, field-by-field comparison, and duplicate detection
- **Results_Display**: The `displayRDBMSResults` frontend function used to render comparison results
- **Primary_Key**: The field name used to join source and target records for comparison

## Requirements

### Requirement 1: Source Custom Query Input Field

**User Story:** As a data engineer, I want to enter a custom BigQuery SQL query for the source data, so that I can validate arbitrary query results instead of only full tables.

#### Acceptance Criteria

1. THE Config_Form SHALL display a Source_Custom_Query_Field textarea in the left-side source configuration panel, positioned after the source filter field
2. THE Source_Custom_Query_Field SHALL have the HTML id `bqvsbq-source-custom-query`
3. THE Source_Custom_Query_Field SHALL include placeholder text indicating the expected SQL format (e.g., `SELECT * FROM \`project.dataset.table\` WHERE condition`)
4. THE Source_Custom_Query_Field SHALL be labeled "Source Custom Query (Optional)"
5. THE Source_Custom_Query_Field SHALL use a monospace font family and allow vertical resizing

### Requirement 2: Target Custom Query Input Field

**User Story:** As a data engineer, I want to enter a custom BigQuery SQL query for the target data, so that I can validate arbitrary query results instead of only full tables.

#### Acceptance Criteria

1. THE Config_Form SHALL display a Target_Custom_Query_Field textarea in the right-side target configuration panel, positioned after the comparison fields input and before the start validation button
2. THE Target_Custom_Query_Field SHALL have the HTML id `bqvsbq-target-custom-query`
3. THE Target_Custom_Query_Field SHALL include placeholder text indicating the expected SQL format (e.g., `SELECT * FROM \`project.dataset.table\` WHERE condition`)
4. THE Target_Custom_Query_Field SHALL be labeled "Target Custom Query (Optional)"
5. THE Target_Custom_Query_Field SHALL use a monospace font family and allow vertical resizing

### Requirement 3: Custom Query Mode Detection

**User Story:** As a data engineer, I want the system to automatically detect when I have provided both custom queries, so that it switches to custom query comparison without extra configuration.

#### Acceptance Criteria

1. WHEN both the Source_Custom_Query_Field and Target_Custom_Query_Field contain non-empty trimmed text, THE Dashboard SHALL activate Custom_Query_Mode
2. WHEN Custom_Query_Mode is active, THE Dashboard SHALL require only the Primary_Key field to be filled
3. WHEN Custom_Query_Mode is active, THE Dashboard SHALL skip validation of source dataset, source table(s), and target dataset fields
4. WHEN only one of the two custom query fields contains text, THE Dashboard SHALL remain in Table_Mode and apply standard table-based validation rules
5. WHEN neither custom query field contains text, THE Dashboard SHALL remain in Table_Mode and apply standard table-based validation rules

### Requirement 4: Custom Query Mode API Call

**User Story:** As a data engineer, I want the custom query comparison to use a dedicated API endpoint, so that the existing table-based comparison logic remains unaffected.

#### Acceptance Criteria

1. WHEN Custom_Query_Mode is active, THE Dashboard SHALL send a POST request to the Custom_Query_Endpoint (`/api/bq-vs-bq-custom`)
2. THE Dashboard SHALL include `sourceQuery`, `targetQuery`, `primaryKey`, and `comparisonFields` in the POST request body
3. WHEN Custom_Query_Mode is active, THE Dashboard SHALL display a loading message reading "Running Custom Query Comparison..."
4. THE Dashboard SHALL display results from the Custom_Query_Endpoint using the Results_Display function with `dbType` set to `bigquery`

### Requirement 5: Custom Query Backend Endpoint

**User Story:** As a data engineer, I want the backend to execute both custom queries against BigQuery and compare the results, so that I get the same detailed validation report as table-based comparison.

#### Acceptance Criteria

1. THE Custom_Query_Endpoint SHALL accept POST requests with `sourceQuery`, `targetQuery`, `primaryKey`, and `comparisonFields` parameters
2. WHEN `sourceQuery`, `targetQuery`, or `primaryKey` is missing, THE Custom_Query_Endpoint SHALL return HTTP 400 with a descriptive error message
3. THE Custom_Query_Endpoint SHALL execute the `sourceQuery` against BigQuery to obtain source data
4. THE Custom_Query_Endpoint SHALL execute the `targetQuery` against BigQuery to obtain target data
5. THE Custom_Query_Endpoint SHALL perform record count comparison between source and target query results
6. THE Custom_Query_Endpoint SHALL perform sample-based primary key matching between source and target results
7. THE Custom_Query_Endpoint SHALL perform field-by-field comparison on matched records using the same logic as the Table_Endpoint
8. THE Custom_Query_Endpoint SHALL perform duplicate primary key detection on both source and target results
9. THE Custom_Query_Endpoint SHALL return a response with the same JSON structure as the Table_Endpoint so that the Results_Display function renders correctly
10. IF a custom query fails to execute, THEN THE Custom_Query_Endpoint SHALL return HTTP 400 with the BigQuery error message and suggestions for resolution

### Requirement 6: Existing Table-Based Flow Preservation

**User Story:** As a data engineer, I want the existing table-based BQ vs BQ comparison to continue working unchanged, so that adding custom query support does not break current functionality.

#### Acceptance Criteria

1. THE Table_Endpoint (`/api/bq-vs-bq`) SHALL remain unchanged in request format, response format, and behavior
2. WHEN Custom_Query_Mode is not active, THE Dashboard SHALL call the Table_Endpoint with the same request structure as before this feature
3. THE Results_Display function SHALL remain unchanged
4. WHEN the Config_Form is submitted with empty custom query fields, THE Dashboard SHALL behave identically to the pre-feature implementation
