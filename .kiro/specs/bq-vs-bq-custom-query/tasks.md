# Implementation Plan: BQ vs BQ Custom Query

## Overview

Add optional custom SQL query support to the BQ vs BQ comparison tab. Two new textareas let users enter arbitrary BigQuery SQL for source and target. When both are filled, the system routes to a new `/api/bq-vs-bq-custom` endpoint that wraps the queries as CTEs and runs the same comparison pipeline. The existing table-based flow and `displayRDBMSResults` remain untouched.

## Tasks

- [x] 1. Add custom query textarea fields to the frontend form
  - [x] 1.1 Add Source Custom Query textarea in the left panel after the source filter field
    - HTML id: `bqvsbq-source-custom-query`
    - Label: "Source Custom Query (Optional)"
    - Placeholder: `SELECT * FROM \`project.dataset.table\` WHERE condition`
    - Monospace font, vertical resize, styled consistently with existing fields
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [x] 1.2 Add Target Custom Query textarea in the right panel after comparison fields input, before the start button
    - HTML id: `bqvsbq-target-custom-query`
    - Label: "Target Custom Query (Optional)"
    - Placeholder: `SELECT * FROM \`project.dataset.table\` WHERE condition`
    - Monospace font, vertical resize, styled consistently with existing fields
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 2. Modify `startBQvsBQComparison()` to support custom query mode
  - [x] 2.1 Add custom query mode detection at the top of the function
    - Read both custom query fields, trim whitespace
    - Set `isCustomQueryMode = !!(sourceCustomQuery && targetCustomQuery)`
    - When custom mode is active: skip dataset/table validation, require only primary key
    - When custom mode is inactive (0 or 1 field filled): existing table-mode validation unchanged
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_
  - [x] 2.2 Add custom query mode API call branch
    - When `isCustomQueryMode` is true, POST to `/api/bq-vs-bq-custom` with `{ sourceQuery, targetQuery, primaryKey, comparisonFields }`
    - Show loading message: "Running Custom Query Comparison..."
    - On success, call `displayRDBMSResults(result, 'bigquery', resultsSection)`
    - On error, display error with suggestions and "New Comparison" button
    - Existing table-mode fetch to `/api/bq-vs-bq` remains in the else branch, unchanged
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 6.2, 6.4_
  - [ ]* 2.3 Write property tests for custom query mode detection and validation (Properties 1, 2, 3)
    - **Property 1: Custom query mode activation** — For any two non-empty trimmed strings in both query fields, mode detection returns true
    - **Validates: Requirements 3.1**
    - **Property 2: Custom mode requires only primary key** — For any form state with both queries filled and PK filled, validation passes regardless of dataset/table fields
    - **Validates: Requirements 3.2, 3.3**
    - **Property 3: Partial or empty custom queries preserve table mode** — For any form state with 0 or 1 query field filled, mode detection returns false
    - **Validates: Requirements 3.4, 3.5, 6.2, 6.4**
  - [ ]* 2.4 Write property test for correct endpoint routing (Property 4)
    - **Property 4: Custom mode routes to correct endpoint with correct payload** — For any custom mode submission, fetch is called with `/api/bq-vs-bq-custom` and body contains sourceQuery, targetQuery, primaryKey, comparisonFields
    - **Validates: Requirements 4.1, 4.2**

- [x] 3. Checkpoint — Verify frontend changes
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement `/api/bq-vs-bq-custom` backend endpoint
  - [x] 4.1 Add the endpoint in server.js after the existing `/api/bq-schema-null-check` endpoint
    - Accept POST with `{ sourceQuery, targetQuery, primaryKey, comparisonFields }`
    - Return 400 if sourceQuery, targetQuery, or primaryKey is missing/empty
    - _Requirements: 5.1, 5.2_
  - [x] 4.2 Implement CTE-based record count comparison
    - Wrap sourceQuery and targetQuery as CTEs: `WITH source_cte AS (<sourceQuery>), target_cte AS (<targetQuery>)`
    - Query `COUNT(*)` from each CTE for source_total and target_total
    - Handle query execution errors with 400 response, BigQuery error message, and suggestions array
    - _Requirements: 5.3, 5.4, 5.5, 5.10_
  - [x] 4.3 Implement sample PK extraction and schema detection
    - Extract up to 2000 distinct PKs from source_cte
    - Detect schema by running `SELECT * FROM source_cte LIMIT 1` and `SELECT * FROM target_cte LIMIT 1`, extracting field names from result keys
    - Determine common fields, fields to compare (all common minus PK if comparisonFields empty)
    - _Requirements: 5.6_
  - [x] 4.4 Implement field-by-field comparison on matched sample records
    - Join source_cte and target_cte on PK for sampled keys
    - Compare each field using SAFE_CAST to STRING, batch fields in groups of 5
    - Calculate perfectMatches, differences, matchRate per field
    - _Requirements: 5.7_
  - [x] 4.5 Implement duplicate PK detection
    - Detect PKs appearing more than once in source_cte and target_cte for sampled keys
    - _Requirements: 5.8_
  - [x] 4.6 Build response with same JSON structure as `/api/bq-vs-bq`
    - Set metadata: `sourceType: 'BIGQUERY_CUSTOM_QUERY'`, `comparisonType: 'BQ-vs-BQ-Custom'`
    - Set `sourceTable` and `targetTable` in metadata to the query text (truncated)
    - Include all required top-level keys: summary, recordCounts, schemaAnalysis, fieldWiseAnalysis, duplicatesAnalysis, metadata
    - Wrap in try/catch with 500 error response for unexpected failures
    - _Requirements: 5.9, 6.1, 6.3_
  - [ ]* 4.7 Write property test for missing required parameters (Property 5)
    - **Property 5: Missing required parameters return HTTP 400** — For any request missing sourceQuery, targetQuery, or primaryKey, endpoint returns 400 with error message
    - **Validates: Requirements 5.2**
  - [ ]* 4.8 Write property tests for comparison correctness (Properties 6, 7, 8)
    - **Property 6: Record count and primary key matching correctness** — For any two record sets with a PK column, counts satisfy matched + source_only = source sample size
    - **Validates: Requirements 5.5, 5.6**
    - **Property 7: Field-by-field comparison correctness** — For matched records, perfectMatches + differences = totalRecords per field, matchRate = (perfectMatches / totalRecords) * 100
    - **Validates: Requirements 5.7**
    - **Property 8: Duplicate primary key detection correctness** — For records with known duplicate PKs, detection matches expected duplicates and counts
    - **Validates: Requirements 5.8**
  - [ ]* 4.9 Write property test for response structure (Property 9)
    - **Property 9: Response structure matches table endpoint shape** — For any successful comparison, response contains success: true and data with all required keys: summary, recordCounts, schemaAnalysis, fieldWiseAnalysis, duplicatesAnalysis, metadata
    - **Validates: Requirements 5.9**

- [x] 5. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The existing `/api/bq-vs-bq` endpoint and `displayRDBMSResults` function must NOT be modified
- The new `/api/bq-vs-bq-custom` endpoint goes after `/api/bq-schema-null-check` in server.js
- Property tests use fast-check library with minimum 100 iterations each
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
