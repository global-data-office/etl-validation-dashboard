# Implementation Plan: BQ Schema-Aware Null Check

## Overview

Add a schema-aware null check to the BQ vs BQ validation flow. This involves a new backend endpoint (`/api/bq-schema-null-check`) that reads full schema metadata from `INFORMATION_SCHEMA.COLUMNS`, compares schema properties between source and target, identifies NOT NULL columns, runs targeted null count queries on the target, and returns structured results. The frontend injects a new "Schema & Nulls" tab into the existing BQ vs BQ results view after `displayRDBMSResults` renders, without modifying any existing functions or endpoints.

## Tasks

- [x] 1. Set up test infrastructure and extract pure logic functions
  - [x] 1.1 Install Jest and fast-check, configure test script in `package.json`
    - Run `npm install --save-dev jest fast-check`
    - Add `"test": "jest"` to `package.json` scripts
    - Create `tests/schema-null-check/` directory
    - _Requirements: N/A (infrastructure)_

  - [x] 1.2 Create `services/schema-null-check.js` with pure logic functions
    - Extract the following pure functions into a standalone module so they can be unit-tested without Express or BigQuery:
      - `compareSchemas(sourceColumns, targetColumns)` — takes two arrays of `{column_name, data_type, is_nullable}`, returns `{columns, summary}` per design
      - `identifyNotNullColumns(sourceColumns, primaryKey)` — returns deduplicated array of column names where `is_nullable === 'NO'` plus the primary key
      - `buildNullValidationResults(notNullColumns, nullCountRow, totalRows)` — takes column list, a row of `COUNTIF` results, and total rows; returns `{results, summary}` per design
      - `validateRequestParams(body)` — validates `sourceTable`, `targetTable`, `primaryKey` presence and format; returns `{valid, error}`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 4.2, 4.3, 4.4, 4.5, 5.2, 5.3_

  - [x] 1.3 Checkpoint — Ensure module exports work and Jest runs
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 2. Write property-based and unit tests for pure logic functions
  - [ ]* 2.1 Write property test: Schema comparison correctly classifies columns
    - **Property 1: Schema comparison correctly classifies columns**
    - **Validates: Requirements 2.1, 2.2, 2.3**
    - File: `tests/schema-null-check/schema-comparison.test.js`
    - Generate random source/target column arrays with `fc.array(fc.record({column_name: fc.string(), data_type: fc.constantFrom('INT64','STRING','FLOAT64','BOOL','TIMESTAMP'), is_nullable: fc.constantFrom('YES','NO')}))`, run `compareSchemas`, verify every source column appears, every target column appears, `status` is correct (`both`/`source_only`/`target_only`), and `dataTypeMatch`/`nullableMatch` booleans are correct

  - [ ]* 2.2 Write property test: Schema comparison summary is consistent with detailed results
    - **Property 2: Schema comparison summary is consistent with detailed results**
    - **Validates: Requirements 2.4**
    - File: `tests/schema-null-check/schema-comparison.test.js`
    - Generate random comparison results, verify `summary.totalColumnsCompared === columns.length`, `summary.dataTypeMismatches` equals count of `both` columns with `dataTypeMatch === false`, etc.

  - [ ]* 2.3 Write property test: NOT NULL column identification includes all source NOT NULL columns plus primary key
    - **Property 3: NOT NULL column identification**
    - **Validates: Requirements 3.1, 3.2, 3.3**
    - File: `tests/schema-null-check/not-null-identification.test.js`
    - Generate random schemas with varying `is_nullable` and a random primary key, verify the returned list contains exactly the NOT NULL columns plus the primary key, with no duplicates

  - [ ]* 2.4 Write property test: Null validation status is PASS iff null count is zero
    - **Property 4: Null validation status correctness**
    - **Validates: Requirements 4.3, 4.4**
    - File: `tests/schema-null-check/null-validation.test.js`
    - Generate random null counts and total rows, verify `status === 'PASS'` iff `nullCount === 0`, verify `nonNullCount === totalRows - nullCount`, verify `nullPercentage` arithmetic

  - [ ]* 2.5 Write property test: Null validation summary is consistent with detailed results
    - **Property 5: Null validation summary consistency**
    - **Validates: Requirements 4.5**
    - File: `tests/schema-null-check/null-validation.test.js`
    - Generate random arrays of validation results, verify summary counts match

  - [ ]* 2.6 Write property test: Input validation rejects missing or malformed parameters
    - **Property 6: Input validation rejects bad requests**
    - **Validates: Requirements 5.2, 5.3**
    - File: `tests/schema-null-check/input-validation.test.js`
    - Generate random request bodies with missing fields or malformed table strings, verify rejection

- [x] 3. Implement the `/api/bq-schema-null-check` endpoint in `server.js`
  - [x] 3.1 Add the POST route after the existing `/api/bq-vs-bq` endpoint (after line ~1760)
    - Import `compareSchemas`, `identifyNotNullColumns`, `buildNullValidationResults`, `validateRequestParams` from `services/schema-null-check.js`
    - Validate request using `validateRequestParams`; return 400 on failure
    - Parse `project.dataset.table` to extract project, dataset, table_name for `INFORMATION_SCHEMA` queries
    - Query `INFORMATION_SCHEMA.COLUMNS` for source: `SELECT column_name, data_type, is_nullable FROM \`project.dataset\`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'table'`
    - Query `INFORMATION_SCHEMA.COLUMNS` for target (same pattern)
    - Call `compareSchemas(sourceColumns, targetColumns)`
    - Call `identifyNotNullColumns(sourceColumns, primaryKey)`
    - If NOT NULL columns exist, run batched null count query: `SELECT COUNT(*) as total_rows, COUNTIF(col1 IS NULL) as null_col1, ... FROM \`targetTable\``
    - Call `buildNullValidationResults(notNullColumns, nullCountRow, totalRows)`
    - Return response JSON per design spec
    - Wrap BigQuery calls in try/catch with descriptive error messages per design error table
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 4.1, 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 4. Checkpoint — Backend endpoint complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement frontend tab injection and rendering
  - [x] 5.1 Add `populateSchemaNullContent(data)` function to `public/index.html`
    - Takes the `data` object from the schema-null-check response
    - Returns an HTML string containing:
      - Summary cards row: total NOT NULL columns checked, columns passed, columns failed, pass rate
      - Schema comparison table: column name, source type, target type, type match (✅/❌), source nullable, target nullable, nullable match (✅/❌), status badge (both/source_only/target_only)
      - Null validation results table: column name, total rows, null count, non-null count, null %, status (PASS ✅ / FAIL ❌)
    - Color-code rows: green for PASS, red for FAIL, yellow for source_only/target_only
    - _Requirements: 6.3, 6.4, 6.5_

  - [x] 5.2 Add `injectSchemaNullTab(schemaNullData, dbType)` function to `public/index.html`
    - Find the `.results-navigation-tabs` container within the BQ results section
    - Append a new tab button: `<button class="results-tab-button" onclick="switchRdbmsTab('${dbType}', 'schema-nulls')">🔎 Schema & Nulls</button>`
    - Find the inner content wrapper div (parent of `rdbms-tab-${dbType}-record-count`)
    - Append a new content div with ID `rdbms-tab-${dbType}-schema-nulls`, `display: none`, class `results-tab-content`
    - Populate content div using `populateSchemaNullContent(schemaNullData)`
    - If `schemaNullData` is an error object, display error message instead
    - _Requirements: 6.1, 6.2, 6.6, 6.7_

  - [x] 5.3 Add `callSchemaNullCheck(sourceTable, targetTable, primaryKey, dbType)` function to `public/index.html`
    - `fetch('/api/bq-schema-null-check', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({sourceTable, targetTable, primaryKey}) })`
    - On success (`response.ok` and `result.success`): call `injectSchemaNullTab(result.data, dbType)`
    - On failure: call `injectSchemaNullTab({error: errorMessage}, dbType)` so the tab shows an error without affecting other tabs
    - _Requirements: 7.1, 7.2, 7.3_

  - [x] 5.4 Integrate `callSchemaNullCheck` into `startBQvsBQComparison()` flow
    - For single-table case: after `displayRDBMSResults(allResults[0].data, 'bigquery', resultsSection)`, call `callSchemaNullCheck(currentSourceTable, currentTargetTable, currentPrimaryKey, 'bigquery')`
    - For multi-table case: inside `viewBQDetailedResults(idx)` (or equivalent), after `displayRDBMSResults` is called for that table, call `callSchemaNullCheck` with that table's metadata
    - Extract `sourceTable`, `targetTable`, `primaryKey` from the stored `allResults` data and `metadata` fields
    - _Requirements: 7.1, 7.4_

- [ ]* 5.5 Write property test: Rendered tab HTML contains all required data fields
    - **Property 7: Rendered tab HTML contains all required data fields**
    - **Validates: Requirements 6.3, 6.4, 6.5**
    - File: `tests/schema-null-check/tab-rendering.test.js`
    - Generate random valid schema-null-check response data, call `populateSchemaNullContent`, verify all column names, data types, match statuses, null counts, pass/fail statuses, and summary values appear in the HTML string

- [x] 6. Final checkpoint — Full integration
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The design uses JavaScript throughout (Node.js + vanilla JS), no language selection needed
- Pure logic is extracted into `services/schema-null-check.js` so it can be tested without BigQuery or Express dependencies
- The `/api/bq-vs-bq` endpoint, `/api/bq-null-check` endpoint, and `displayRDBMSResults` function must NOT be modified
- Property tests validate universal correctness properties from the design document
- Checkpoints ensure incremental validation at backend-complete and full-integration stages
