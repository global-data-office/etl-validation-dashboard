# Requirements Document

## Introduction

This feature adds a schema-aware null check capability to the BQ vs BQ tab of the ETL Validation Dashboard. Today, the BQ vs BQ comparison reads only `column_name` from `INFORMATION_SCHEMA.COLUMNS` and has no null validation tied to source schema constraints. The standalone null-check endpoint does brute-force null counting with no schema awareness. This feature bridges that gap by reading full schema metadata (data types, nullability) from both source and target BQ tables, comparing schema properties, identifying NOT NULL columns in the source, and running targeted null count validation on those columns in the target. Results are displayed in a new dedicated tab within the BQ vs BQ results view, completely isolated from existing functionality.

## Glossary

- **Dashboard**: The ETL Data Validation Dashboard (Node.js/Express backend, single-page HTML frontend at `public/index.html`)
- **BQ_vs_BQ_Tab**: The existing BigQuery vs BigQuery comparison page in the Dashboard, served by the `/api/bq-vs-bq` endpoint
- **Schema_Aware_Null_Check_Endpoint**: A new POST endpoint `/api/bq-schema-null-check` in `server.js` that performs schema comparison and targeted null validation
- **Schema_Comparison**: The process of reading `column_name`, `data_type`, and `is_nullable` from `INFORMATION_SCHEMA.COLUMNS` for both source and target BQ tables and comparing them
- **Targeted_Null_Validation**: Running `COUNT(*) WHERE column IS NULL` queries only on columns identified as NOT NULL in the source schema
- **NOT_NULL_Column**: A column whose `is_nullable` value is `'NO'` in the source table's `INFORMATION_SCHEMA.COLUMNS`
- **Schema_Null_Check_Tab**: A new tab added to the BQ vs BQ results view that displays schema comparison and targeted null check results
- **Source_Table**: The BigQuery source table specified in the BQ vs BQ configuration (format: `project.dataset.table`)
- **Target_Table**: The BigQuery target table specified in the BQ vs BQ configuration (format: `project.dataset.table`)
- **Primary_Key**: The column used to uniquely identify records, as specified in the BQ vs BQ configuration

## Requirements

### Requirement 1: Read Full Schema Metadata from Source and Target

**User Story:** As a QA engineer, I want the system to read full schema metadata (column name, data type, nullability) from both source and target BQ tables, so that I can understand the structural properties of each table.

#### Acceptance Criteria

1. WHEN a schema-aware null check is requested, THE Schema_Aware_Null_Check_Endpoint SHALL query `INFORMATION_SCHEMA.COLUMNS` for the Source_Table and retrieve `column_name`, `data_type`, and `is_nullable` for every column
2. WHEN a schema-aware null check is requested, THE Schema_Aware_Null_Check_Endpoint SHALL query `INFORMATION_SCHEMA.COLUMNS` for the Target_Table and retrieve `column_name`, `data_type`, and `is_nullable` for every column
3. IF the schema query for the Source_Table fails, THEN THE Schema_Aware_Null_Check_Endpoint SHALL return an error response with a descriptive message including the table name
4. IF the schema query for the Target_Table fails, THEN THE Schema_Aware_Null_Check_Endpoint SHALL return an error response with a descriptive message including the table name

### Requirement 2: Compare Schema Properties Between Source and Target

**User Story:** As a QA engineer, I want to see a side-by-side comparison of schema properties between source and target tables, so that I can identify structural discrepancies introduced during ETL.

#### Acceptance Criteria

1. THE Schema_Aware_Null_Check_Endpoint SHALL produce a per-column comparison that includes: column name, source data type, target data type, whether data types match, source `is_nullable` value, target `is_nullable` value, and whether nullability matches
2. WHEN a column exists in the Source_Table but not in the Target_Table, THE Schema_Aware_Null_Check_Endpoint SHALL flag that column as "source only" in the schema comparison results
3. WHEN a column exists in the Target_Table but not in the Source_Table, THE Schema_Aware_Null_Check_Endpoint SHALL flag that column as "target only" in the schema comparison results
4. THE Schema_Aware_Null_Check_Endpoint SHALL include summary counts: total columns compared, columns with data type mismatches, and columns with nullability mismatches

### Requirement 3: Identify NOT NULL Columns from Source Schema

**User Story:** As a QA engineer, I want the system to automatically identify which columns have a NOT NULL constraint in the source, so that I know which columns require null validation in the target.

#### Acceptance Criteria

1. THE Schema_Aware_Null_Check_Endpoint SHALL identify all columns in the Source_Table where `is_nullable` equals `'NO'`
2. THE Schema_Aware_Null_Check_Endpoint SHALL include the Primary_Key column in the NOT NULL validation list regardless of its `is_nullable` metadata value
3. THE Schema_Aware_Null_Check_Endpoint SHALL return the list of identified NOT_NULL_Columns in the response

### Requirement 4: Run Targeted Null Count Validation on Target Table

**User Story:** As a QA engineer, I want the system to run null count queries only on columns that are NOT NULL in the source, so that I can verify the target table preserves data integrity constraints.

#### Acceptance Criteria

1. WHEN NOT_NULL_Columns have been identified, THE Schema_Aware_Null_Check_Endpoint SHALL run a null count query against the Target_Table for each NOT_NULL_Column
2. THE Schema_Aware_Null_Check_Endpoint SHALL return, for each validated column: column name, total row count in the target, null count, non-null count, and null percentage
3. WHEN a NOT_NULL_Column has zero nulls in the Target_Table, THE Schema_Aware_Null_Check_Endpoint SHALL mark that column's validation status as "PASS"
4. WHEN a NOT_NULL_Column has one or more nulls in the Target_Table, THE Schema_Aware_Null_Check_Endpoint SHALL mark that column's validation status as "FAIL" and include the null count
5. THE Schema_Aware_Null_Check_Endpoint SHALL include a summary with: total columns validated, columns passed, columns failed, and overall pass rate as a percentage

### Requirement 5: New Backend Endpoint Isolation

**User Story:** As a developer, I want the schema-aware null check to be a separate endpoint, so that existing BQ vs BQ and null-check functionality remain unaffected.

#### Acceptance Criteria

1. THE Schema_Aware_Null_Check_Endpoint SHALL be a new POST route at `/api/bq-schema-null-check` in `server.js`
2. THE Schema_Aware_Null_Check_Endpoint SHALL accept `sourceTable`, `targetTable`, and `primaryKey` as required request body parameters
3. THE Schema_Aware_Null_Check_Endpoint SHALL validate that `sourceTable` and `targetTable` match the format `project.dataset.table`
4. THE existing `/api/bq-vs-bq` endpoint response structure SHALL remain unchanged
5. THE existing data validation stored procedure call endpoint SHALL remain unchanged

### Requirement 6: Display Results in a New Tab in BQ vs BQ Results

**User Story:** As a QA engineer, I want to see schema comparison and null check results in a new tab within the BQ vs BQ results view, so that I can access the information alongside existing validation results.

#### Acceptance Criteria

1. WHEN BQ vs BQ results are displayed for `dbType` equal to `'bigquery'`, THE Dashboard SHALL render an additional tab labeled "Schema & Nulls" after the existing Duplicates tab
2. THE Schema_Null_Check_Tab SHALL use the tab ID pattern `rdbms-tab-bigquery-schema-nulls` to be compatible with the existing `switchRdbmsTab` function
3. THE Schema_Null_Check_Tab SHALL display a schema comparison table showing: column name, source data type, target data type, type match status, source nullable, target nullable, nullable match status
4. THE Schema_Null_Check_Tab SHALL display a null validation results table showing: column name, total rows, null count, non-null count, null percentage, and pass/fail status
5. THE Schema_Null_Check_Tab SHALL display summary cards showing: total NOT NULL columns checked, columns passed, columns failed, and overall pass rate
6. THE existing `displayRDBMSResults` function SHALL remain unmodified
7. THE existing five tabs (Record Count, Column Names, Comparison, Field Analysis, Duplicates) SHALL remain unchanged in structure and behavior

### Requirement 7: Frontend Invocation of Schema-Aware Null Check

**User Story:** As a QA engineer, I want the schema-aware null check to run automatically as part of the BQ vs BQ validation flow, so that I do not need to trigger it separately.

#### Acceptance Criteria

1. WHEN the BQ vs BQ comparison completes successfully for a single table, THE Dashboard SHALL automatically call the Schema_Aware_Null_Check_Endpoint with the same `sourceTable`, `targetTable`, and `primaryKey` values
2. WHEN the schema-aware null check response is received, THE Dashboard SHALL inject the Schema_Null_Check_Tab into the rendered BQ vs BQ results
3. IF the schema-aware null check call fails, THEN THE Dashboard SHALL display an error message inside the Schema_Null_Check_Tab without affecting the other five tabs
4. WHEN BQ vs BQ validation runs for multiple tables, THE Dashboard SHALL call the Schema_Aware_Null_Check_Endpoint for each table and include the Schema_Null_Check_Tab in each table's detailed results view
