// services/comparison-engine.js - COMPLETE FILE with BigQuery Filtering Support
const { BigQuery } = require('@google-cloud/bigquery');

class ComparisonEngineService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });

        console.log('Comparison Engine Service initialized - FIXED VERSION with Universal Data Types');
        console.log('FIXED: SQL table aliasing + universal data type support + BigQuery filtering for ALL field comparisons');
    }

    cleanFieldName(fieldName) {
        return fieldName
            .replace(/[^a-zA-Z0-9_]/g, '_')
            .replace(/^[0-9]/, '_$&')
            .substring(0, 128)
            .toLowerCase();
    }

    /**
     * Get data types for fields in both tables
     */
    async getFieldDataTypes(tempTableId, sourceTableName, fieldName) {
        try {
            console.log(`Getting data types for field: ${fieldName}`);

            const tempParts = tempTableId.split('.');
            const sourceParts = sourceTableName.split('.');

            const tempProject = tempParts[0];
            const tempDataset = tempParts[1];
            const tempTable = tempParts[2];

            const sourceProject = sourceParts[0];
            const sourceDataset = sourceParts[1];
            const sourceTableName_clean = sourceParts[2];

            const tempTypeQuery = `
                SELECT
                    column_name,
                    data_type,
                    is_nullable
                FROM \`${tempProject}\`.${tempDataset}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '${tempTable}'
                AND column_name = '${fieldName}'
            `;

            const sourceTypeQuery = `
                SELECT
                    column_name,
                    data_type,
                    is_nullable
                FROM \`${sourceProject}\`.${sourceDataset}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '${sourceTableName_clean}'
                AND column_name = '${fieldName}'
            `;

            let tempType = 'STRING';
            let sourceType = 'STRING';

            try {
                const [tempTypeResult] = await this.bigquery.query(tempTypeQuery);
                const [sourceTypeResult] = await this.bigquery.query(sourceTypeQuery);

                if (tempTypeResult.length > 0) tempType = tempTypeResult[0].data_type;
                if (sourceTypeResult.length > 0) sourceType = sourceTypeResult[0].data_type;

                console.log(`Data types detected for ${fieldName} - Temp: ${tempType}, Source: ${sourceType}`);

            } catch (typeError) {
                console.warn(`Could not get schema info for ${fieldName}, using default STRING types:`, typeError.message);
            }

            return { tempType, sourceType };

        } catch (error) {
            console.warn(`Data type detection failed for ${fieldName}, using STRING fallback:`, error.message);
            return { tempType: 'STRING', sourceType: 'STRING' };
        }
    }

    /**
     * Generate casting expression for any BigQuery data type
     */
    getCastExpression(fieldName, sourceType, targetType) {
        if (sourceType === targetType) {
            return fieldName;
        }

        switch (targetType) {
            case 'STRING':
                return `SAFE_CAST(${fieldName} AS STRING)`;
            case 'INT64':
            case 'INTEGER':
                return `SAFE_CAST(${fieldName} AS INT64)`;
            case 'FLOAT64':
            case 'FLOAT':
                return `SAFE_CAST(${fieldName} AS FLOAT64)`;
            case 'NUMERIC':
                return `SAFE_CAST(${fieldName} AS NUMERIC)`;
            case 'BIGNUMERIC':
                return `SAFE_CAST(${fieldName} AS BIGNUMERIC)`;
            case 'BOOLEAN':
            case 'BOOL':
                return `SAFE_CAST(${fieldName} AS BOOL)`;
            case 'DATE':
                return `SAFE_CAST(${fieldName} AS DATE)`;
            case 'DATETIME':
                return `SAFE_CAST(${fieldName} AS DATETIME)`;
            case 'TIMESTAMP':
                return `SAFE_CAST(${fieldName} AS TIMESTAMP)`;
            case 'TIME':
                return `SAFE_CAST(${fieldName} AS TIME)`;
            case 'BYTES':
                return `SAFE_CAST(${fieldName} AS BYTES)`;
            case 'JSON':
                return `SAFE_CAST(${fieldName} AS JSON)`;
            case 'GEOGRAPHY':
                return `SAFE_CAST(${fieldName} AS GEOGRAPHY)`;
            default:
                return `SAFE_CAST(${fieldName} AS STRING)`;
        }
    }

    /**
     * Determine best common type for comparison between two types
     */
    getBestCommonType(type1, type2) {
        if (type1 === type2) {
            return type1;
        }

        const numericTypes = ['INT64', 'INTEGER', 'FLOAT64', 'FLOAT', 'NUMERIC', 'BIGNUMERIC'];
        if (numericTypes.includes(type1) && numericTypes.includes(type2)) {
            if (type1.includes('FLOAT') || type2.includes('FLOAT')) return 'FLOAT64';
            if (type1.includes('NUMERIC') || type2.includes('NUMERIC')) return 'NUMERIC';
            return 'INT64';
        }

        const dateTypes = ['DATE', 'DATETIME', 'TIMESTAMP'];
        if (dateTypes.includes(type1) && dateTypes.includes(type2)) {
            return 'STRING';
        }

        if ((type1 === 'BOOLEAN' || type1 === 'BOOL') && (type2 === 'BOOLEAN' || type2 === 'BOOL')) {
            return 'BOOL';
        }

        return 'STRING';
    }

    /**
     * Get common fields with proper field detection
     */
    async getCommonFields(tempTableId, sourceTableReference) {
    try {
        console.log('Starting field analysis...');
        console.log(`Temp table: ${tempTableId}`);
        console.log(`Source table reference: ${sourceTableReference}`);

        let jsonFields = [];
        try {
            const jsonSchemaQuery = `SELECT * FROM \`${tempTableId}\` LIMIT 1`;
            const [jsonSample] = await this.bigquery.query(jsonSchemaQuery);

            if (jsonSample.length > 0) {
                jsonFields = Object.keys(jsonSample[0]).sort();
                console.log(`JSON table has ${jsonFields.length} fields`);
            } else {
                throw new Error('JSON temp table is empty');
            }
        } catch (jsonError) {
            console.error('ERROR getting JSON fields:', jsonError.message);
            throw new Error(`Cannot access JSON temp table: ${jsonError.message}`);
        }

        let bqFields = [];
        try {
            // FIXED: Handle both simple table references and complex subqueries
            let bqSchemaQuery;

            if (sourceTableReference.includes('SELECT') && sourceTableReference.includes('AS filtered_bq_table')) {
                // This is a filtered subquery - extract the original table name for schema analysis
                const tableMatch = sourceTableReference.match(/FROM\s+`([^`]+)`/);
                if (tableMatch) {
                    const originalTable = tableMatch[1];
                    console.log(`FIXED: Detected filtered subquery, using original table for schema: ${originalTable}`);
                    bqSchemaQuery = `SELECT * FROM \`${originalTable}\` LIMIT 1`;
                } else {
                    console.warn('Could not extract original table from filtered query, trying direct query');
                    bqSchemaQuery = `SELECT * FROM ${sourceTableReference} LIMIT 1`;
                }
            } else {
                // Simple table reference
                bqSchemaQuery = `SELECT * FROM \`${sourceTableReference}\` LIMIT 1`;
            }

            console.log(`BigQuery schema query: ${bqSchemaQuery}`);
            const [bqSample] = await this.bigquery.query(bqSchemaQuery);

            if (bqSample.length > 0) {
                bqFields = Object.keys(bqSample[0]).sort();
                console.log(`BigQuery table has ${bqFields.length} fields`);
            } else {
                throw new Error('BigQuery table is empty');
            }
        } catch (bqError) {
            console.error('ERROR getting BigQuery fields:', bqError.message);
            throw new Error(`Cannot access BigQuery source table: ${bqError.message}`);
        }

        const commonFields = [];
        const jsonOnlyFields = [];
        const bqOnlyFields = [];

        for (const jsonField of jsonFields) {
            if (bqFields.includes(jsonField)) {
                commonFields.push(jsonField);
            } else {
                jsonOnlyFields.push(jsonField);
            }
        }

        for (const bqField of bqFields) {
            if (!jsonFields.includes(bqField)) {
                bqOnlyFields.push(bqField);
            }
        }

        console.log(`Common fields found: ${commonFields.length}`);
        console.log(`Common fields: [${commonFields.join(', ')}]`);

        const primaryKeyCandidates = commonFields.filter(field => {
            const lowerField = field.toLowerCase();
            return lowerField.includes('id') ||
                   lowerField.includes('key') ||
                   lowerField.includes('number') ||
                   lowerField.includes('arn') ||
                   ['id', 'sys_id', 'number', 'key', 'code', 'arn'].includes(lowerField);
        });

        if (commonFields.length === 0) {
            console.error('No common fields detected!');

            const caseInsensitiveMatches = [];
            for (const jsonField of jsonFields) {
                const matchingBqField = bqFields.find(bqField =>
                    bqField.toLowerCase() === jsonField.toLowerCase()
                );
                if (matchingBqField) {
                    caseInsensitiveMatches.push(matchingBqField);
                }
            }

            if (caseInsensitiveMatches.length > 0) {
                return {
                    commonFields: caseInsensitiveMatches,
                    jsonOnlyFields: jsonFields.filter(jf =>
                        !caseInsensitiveMatches.some(cf => cf.toLowerCase() === jf.toLowerCase())
                    ),
                    bqOnlyFields: bqFields.filter(bf =>
                        !caseInsensitiveMatches.some(cf => cf.toLowerCase() === bf.toLowerCase())
                    ),
                    primaryKeyCandidates: caseInsensitiveMatches.filter(field => {
                        const lowerField = field.toLowerCase();
                        return lowerField.includes('id') || lowerField.includes('arn');
                    }),
                    totalJsonFields: jsonFields.length,
                    totalBqFields: bqFields.length,
                    schemaCompatibility: caseInsensitiveMatches.length / Math.max(jsonFields.length, bqFields.length),
                    matchType: 'case-insensitive'
                };
            }

            throw new Error(`No common fields found. JSON has [${jsonFields.slice(0, 5).join(', ')}], BigQuery has [${bqFields.slice(0, 5).join(', ')}]`);
        }

        return {
            commonFields: commonFields,
            jsonOnlyFields: jsonOnlyFields,
            bqOnlyFields: bqOnlyFields,
            primaryKeyCandidates: primaryKeyCandidates,
            totalJsonFields: jsonFields.length,
            totalBqFields: bqFields.length,
            schemaCompatibility: commonFields.length / Math.max(jsonFields.length, bqFields.length),
            jsonColumns: jsonFields.map(field => ({ column_name: field, data_type: 'STRING' })),
            bqColumns: bqFields.map(field => ({ column_name: field, data_type: 'STRING' })),
            commonColumns: commonFields.map(field => ({
                column_name: field,
                json_type: 'STRING',
                bq_type: 'STRING',
                type_match: true
            })),
            matchType: 'exact'
        };

    } catch (error) {
        console.error('Schema analysis failed:', error.message);
        throw new Error(`Schema analysis failed: ${error.message}`);
    }
}


    /**
     * Validate ANY field as primary key
     */
    async validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, commonFields) {
        try {
            console.log(`Validating primary key field: ${primaryKey}`);

            if (!commonFields.includes(primaryKey)) {
                console.error(`Primary key '${primaryKey}' not found in common fields`);
                throw new Error(`Primary key '${primaryKey}' not available in both tables. Available common fields: ${commonFields.slice(0, 10).join(', ')}`);
            }

            console.log(`Primary key '${primaryKey}' validated - exists in both tables`);

            const validationQueries = [
                {
                    name: 'JSON Table Stats',
                    query: `
                        SELECT
                            COUNT(*) as total_count,
                            COUNT(${primaryKey}) as non_null_count,
                            COUNT(DISTINCT ${primaryKey}) as unique_count
                        FROM \`${tempTableId}\`
                    `
                },
                {
                    name: 'BigQuery Table Stats',
                    query: `
                        SELECT
                            COUNT(*) as total_count,
                            COUNT(${primaryKey}) as non_null_count,
                            COUNT(DISTINCT ${primaryKey}) as unique_count
                        FROM \`${sourceTableName}\`
                    `
                }
            ];

            const validationResults = {};

            for (const queryInfo of validationQueries) {
                try {
                    const [result] = await this.bigquery.query(queryInfo.query);
                    validationResults[queryInfo.name] = result[0];
                } catch (queryError) {
                    console.error(`${queryInfo.name} failed:`, queryError.message);
                    throw new Error(`Field '${primaryKey}' validation failed: ${queryError.message}`);
                }
            }

            return validationResults;

        } catch (error) {
            console.error(`Primary key validation failed:`, error.message);
            throw error;
        }
    }

    /**
     * FIXED: Find matches with proper table aliasing and universal data types
     */
    async getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`Finding matches using primary key: ${primaryKey}`);

            const dataTypes = await this.getFieldDataTypes(tempTableId, sourceTableName, primaryKey);
            const commonType = this.getBestCommonType(dataTypes.tempType, dataTypes.sourceType);

            console.log(`Using common type for comparison: ${commonType}`);

            const tempCast = this.getCastExpression('json_table.' + primaryKey, dataTypes.tempType, commonType);
            const sourceCast = this.getCastExpression('bq_table.' + primaryKey, dataTypes.sourceType, commonType);

            // Get all unique JSON keys with type casting
            const getAllJsonKeysQuery = `
                SELECT DISTINCT ${this.getCastExpression(primaryKey, dataTypes.tempType, commonType)} as key_value
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                ORDER BY key_value
                LIMIT 10000
            `;

            const [allJsonKeys] = await this.bigquery.query(getAllJsonKeysQuery);
            const jsonKeysList = allJsonKeys.map(r => r.key_value);

            console.log(`JSON unique keys: ${jsonKeysList.length} found`);

            if (jsonKeysList.length === 0) {
                console.warn('No valid keys found in JSON table');
                return {
                    matchCount: 0,
                    matchedIds: [],
                    jsonOnlyCount: 0,
                    jsonOnlyIds: [],
                    jsonOnlyRecords: [],
                    bqOnlyCount: 0,
                    bqOnlyRecords: [],
                    sampleMatches: [],
                    primaryKeyUsed: primaryKey,
                    dataTypes: dataTypes
                };
            }

            // FIXED: Find matching keys with proper table aliasing
            const getMatchingKeysQuery = `
                SELECT DISTINCT ${tempCast} as matched_key
                FROM \`${tempTableId}\` json_table
                INNER JOIN \`${sourceTableName}\` bq_table
                ON ${tempCast} = ${sourceCast}
                WHERE json_table.${primaryKey} IS NOT NULL
                AND bq_table.${primaryKey} IS NOT NULL
                ORDER BY matched_key
                LIMIT 10000
            `;

            const [matchingKeys] = await this.bigquery.query(getMatchingKeysQuery);
            const matchedKeysList = matchingKeys.map(r => r.matched_key);

            console.log(`Matched keys: ${matchedKeysList.length} found`);

            const jsonOnlyKeys = jsonKeysList.filter(key => !matchedKeysList.includes(key));
            console.log(`JSON-only keys: ${jsonOnlyKeys.length} found`);

            // FIXED: Get sample keys only in BigQuery with proper aliasing
            const getBqOnlyKeysQuery = `
                SELECT DISTINCT ${this.getCastExpression(primaryKey, dataTypes.sourceType, commonType)} as bq_only_key
                FROM \`${sourceTableName}\` bq_table
                WHERE bq_table.${primaryKey} IS NOT NULL
                  AND ${this.getCastExpression(primaryKey, dataTypes.sourceType, commonType)} NOT IN (
                    SELECT DISTINCT ${this.getCastExpression(primaryKey, dataTypes.tempType, commonType)}
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;

            const [bqOnlyKeys] = await this.bigquery.query(getBqOnlyKeysQuery);
            const bqOnlyKeysList = bqOnlyKeys.map(r => r.bq_only_key);

            console.log(`BigQuery-only keys (sample): ${bqOnlyKeysList.length} found`);

            let sampleMatches = [];
            if (matchedKeysList.length > 0) {
                // FIXED: Sample query with proper table aliasing
                const sampleMatchQuery = `
                    SELECT
                        ${tempCast} as key_value
                    FROM \`${tempTableId}\` json_table
                    INNER JOIN \`${sourceTableName}\` bq_table
                    ON ${tempCast} = ${sourceCast}
                    WHERE json_table.${primaryKey} IS NOT NULL
                    AND bq_table.${primaryKey} IS NOT NULL
                    LIMIT 5
                `;

                try {
                    const [sampleResult] = await this.bigquery.query(sampleMatchQuery);
                    sampleMatches = sampleResult;
                    console.log(`Sample matches retrieved: ${sampleMatches.length} records`);
                } catch (sampleError) {
                    console.warn(`Could not get sample match details:`, sampleError.message);
                }
            }

            return {
                matchCount: matchedKeysList.length,
                matchedIds: matchedKeysList,
                jsonOnlyCount: jsonOnlyKeys.length,
                jsonOnlyIds: jsonOnlyKeys,
                jsonOnlyRecords: jsonOnlyKeys.map(key => ({ [primaryKey]: key })),
                bqOnlyCount: bqOnlyKeysList.length,
                bqOnlyRecords: bqOnlyKeysList.map(key => ({ [primaryKey]: key })),
                sampleMatches: sampleMatches,
                primaryKeyUsed: primaryKey,
                dataTypes: dataTypes
            };

        } catch (error) {
            console.error(`Match analysis failed:`, error.message);
            throw new Error(`Match analysis failed: ${error.message}`);
        }
    }

    /**
     * ENHANCED: Comprehensive duplicates analysis for BOTH systems
     */
    async analyzeBothSystemDuplicates(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`Analyzing duplicates in BOTH systems using primary key: ${primaryKey}`);

            const dataTypes = await this.getFieldDataTypes(tempTableId, sourceTableName, primaryKey);
            const commonType = this.getBestCommonType(dataTypes.tempType, dataTypes.sourceType);

            const tempCast = this.getCastExpression(primaryKey, dataTypes.tempType, commonType);
            const sourceCast = this.getCastExpression(primaryKey, dataTypes.sourceType, commonType);

            // Analyze JSON duplicates
            const jsonDuplicateQuery = `
                SELECT
                    ${tempCast} as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY ${tempCast}
                HAVING COUNT(*) > 1
                ORDER BY occurrence_count DESC
                LIMIT 100
            `;

            // Analyze BigQuery duplicates
            const bqDuplicateQuery = `
                SELECT
                    ${sourceCast} as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM \`${sourceTableName}\`
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY ${sourceCast}
                HAVING COUNT(*) > 1
                ORDER BY occurrence_count DESC
                LIMIT 100
            `;

            const [jsonDuplicateResult, bqDuplicateResult] = await Promise.all([
                this.bigquery.query(jsonDuplicateQuery).catch(error => {
                    console.warn('JSON duplicate analysis failed:', error.message);
                    return [[]];
                }),
                this.bigquery.query(bqDuplicateQuery).catch(error => {
                    console.warn('BigQuery duplicate analysis failed:', error.message);
                    return [[]];
                })
            ]);

            const jsonDuplicates = jsonDuplicateResult[0] || [];
            const bqDuplicates = bqDuplicateResult[0] || [];

            console.log(`JSON duplicates found: ${jsonDuplicates.length}`);
            console.log(`BigQuery duplicates found: ${bqDuplicates.length}`);

            const jsonDuplicateRecordCount = jsonDuplicates.reduce((sum, dup) => sum + parseInt(dup.occurrence_count), 0) - jsonDuplicates.length;
            const bqDuplicateRecordCount = bqDuplicates.reduce((sum, dup) => sum + parseInt(dup.occurrence_count), 0) - bqDuplicates.length;

            const jsonDuplicateKeys = new Set(jsonDuplicates.map(dup => String(dup.duplicate_key)));
            const bqDuplicateKeys = new Set(bqDuplicates.map(dup => String(dup.duplicate_key)));

            const commonDuplicateKeys = [...jsonDuplicateKeys].filter(key => bqDuplicateKeys.has(key));
            const jsonOnlyDuplicateKeys = [...jsonDuplicateKeys].filter(key => !bqDuplicateKeys.has(key));
            const bqOnlyDuplicateKeys = [...bqDuplicateKeys].filter(key => !jsonDuplicateKeys.has(key));

            console.log(`Common duplicate keys: ${commonDuplicateKeys.length}`);

            // Create sample keys for UI display
            const jsonSampleKeys = jsonDuplicates.slice(0, 20).map(dup => `${dup.duplicate_key} (${dup.occurrence_count}x)`);
            const bqSampleKeys = bqDuplicates.slice(0, 20).map(dup => `${dup.duplicate_key} (${dup.occurrence_count}x)`);

            const recommendations = [];

            if (jsonDuplicates.length === 0 && bqDuplicates.length === 0) {
                recommendations.push("Excellent data quality - No duplicate keys found in either system");
                recommendations.push("Both JSON source and BigQuery target have unique primary key values");
            } else {
                if (jsonDuplicates.length > 0) {
                    recommendations.push(`JSON Source: Found ${jsonDuplicates.length} duplicate key values affecting ${jsonDuplicateRecordCount} records`);
                    recommendations.push("Consider implementing deduplication logic in your JSON data source");
                }

                if (bqDuplicates.length > 0) {
                    recommendations.push(`BigQuery Target: Found ${bqDuplicates.length} duplicate key values affecting ${bqDuplicateRecordCount} records`);
                    recommendations.push("Review BigQuery table loading process to prevent duplicate key insertion");
                }

                if (commonDuplicateKeys.length > 0) {
                    recommendations.push(`Critical: ${commonDuplicateKeys.length} duplicate keys exist in BOTH systems`);
                    recommendations.push("This indicates systemic data quality issues requiring immediate attention");
                }
            }

            return {
                jsonDuplicates: {
                    hasDuplicates: jsonDuplicates.length > 0,
                    duplicateCount: jsonDuplicates.length,
                    count: jsonDuplicates.length, // alias for UI compatibility
                    totalDuplicateRecords: jsonDuplicateRecordCount,
                    sampleKeys: jsonSampleKeys, // for UI display
                    samples: jsonSampleKeys, // alias for UI compatibility
                    duplicateKeys: jsonDuplicates.map(dup => ({
                        key: dup.duplicate_key,
                        count: parseInt(dup.occurrence_count),
                        occurrences: parseInt(dup.occurrence_count)
                    }))
                },

                bqDuplicates: {
                    hasDuplicates: bqDuplicates.length > 0,
                    duplicateCount: bqDuplicates.length,
                    count: bqDuplicates.length, // alias for UI compatibility
                    totalDuplicateRecords: bqDuplicateRecordCount,
                    sampleKeys: bqSampleKeys, // for UI display
                    samples: bqSampleKeys, // alias for UI compatibility
                    duplicateKeys: bqDuplicates.map(dup => ({
                        key: dup.duplicate_key,
                        count: parseInt(dup.occurrence_count),
                        occurrences: parseInt(dup.occurrence_count)
                    }))
                },

                crossSystemAnalysis: {
                    commonDuplicateKeys: commonDuplicateKeys,
                    jsonOnlyDuplicateKeys: jsonOnlyDuplicateKeys,
                    bqOnlyDuplicateKeys: bqOnlyDuplicateKeys
                },

                summary: {
                    totalSystemsWithDuplicates: (jsonDuplicates.length > 0 ? 1 : 0) + (bqDuplicates.length > 0 ? 1 : 0),
                    totalDuplicateKeys: jsonDuplicates.length + bqDuplicates.length,
                    totalDuplicateRecords: jsonDuplicateRecordCount + bqDuplicateRecordCount,
                    bothSystemsClean: jsonDuplicates.length === 0 && bqDuplicates.length === 0,
                    criticalIssues: commonDuplicateKeys.length,
                    dataQualityScore: commonDuplicateKeys.length === 0 ? 'Good' : 'Needs Attention'
                },

                recommendations: recommendations,
                dataTypes: dataTypes
            };

        } catch (error) {
            console.error('Complete duplicates analysis failed:', error.message);
            return {
                jsonDuplicates: { hasDuplicates: false, duplicateCount: 0, count: 0, totalDuplicateRecords: 0, duplicateKeys: [], sampleKeys: [], samples: [] },
                bqDuplicates: { hasDuplicates: false, duplicateCount: 0, count: 0, totalDuplicateRecords: 0, duplicateKeys: [], sampleKeys: [], samples: [] },
                crossSystemAnalysis: { commonDuplicateKeys: [], jsonOnlyDuplicateKeys: [], bqOnlyDuplicateKeys: [] },
                summary: { totalSystemsWithDuplicates: 0, totalDuplicateKeys: 0, totalDuplicateRecords: 0, bothSystemsClean: true, criticalIssues: 0, dataQualityScore: 'Unknown' },
                recommendations: [`Duplicates analysis failed: ${error.message}`]
            };
        }
    }

    /**
     * Get record counts using dynamic primary key
     */
    async getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`Getting record counts using: ${primaryKey}`);

            const jsonDetailQuery = `
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ${primaryKey}) as unique_primary_keys,
                    COUNT(${primaryKey}) as non_null_primary_keys,
                    COUNT(*) - COUNT(${primaryKey}) as null_primary_keys,
                    COUNT(*) - COUNT(DISTINCT ${primaryKey}) as duplicate_records
                FROM \`${tempTableId}\`
            `;

            const bqDetailQuery = `
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ${primaryKey}) as unique_primary_keys,
                    COUNT(${primaryKey}) as non_null_primary_keys,
                    COUNT(*) - COUNT(${primaryKey}) as null_primary_keys,
                    COUNT(*) - COUNT(DISTINCT ${primaryKey}) as duplicate_records
                FROM \`${sourceTableName}\`
            `;

            const [jsonResult, bqResult] = await Promise.all([
                this.bigquery.query(jsonDetailQuery),
                this.bigquery.query(bqDetailQuery)
            ]);

            const jsonDetails = jsonResult[0][0];
            const bqDetails = bqResult[0][0];

            console.log(`JSON analysis: ${jsonDetails.total_records} total, ${jsonDetails.unique_primary_keys} unique, ${jsonDetails.duplicate_records} duplicates`);
            console.log(`BigQuery analysis: ${bqDetails.total_records} total, ${bqDetails.unique_primary_keys} unique`);

            return {
                jsonDetails: {
                    totalRecords: parseInt(jsonDetails.total_records),
                    uniquePrimaryKeys: parseInt(jsonDetails.unique_primary_keys),
                    nonNullPrimaryKeys: parseInt(jsonDetails.non_null_primary_keys),
                    nullPrimaryKeys: parseInt(jsonDetails.null_primary_keys),
                    duplicateRecords: parseInt(jsonDetails.duplicate_records),
                    primaryKeyField: primaryKey
                },
                bqDetails: {
                    totalRecords: parseInt(bqDetails.total_records),
                    uniquePrimaryKeys: parseInt(bqDetails.unique_primary_keys),
                    nonNullPrimaryKeys: parseInt(bqDetails.non_null_primary_keys),
                    nullPrimaryKeys: parseInt(bqDetails.null_primary_keys),
                    duplicateRecords: parseInt(bqDetails.duplicate_records),
                    primaryKeyField: primaryKey
                }
            };

        } catch (error) {
            console.error(`Failed to get record counts:`, error.message);
            throw error;
        }
    }

    /**
     * FIXED: Analyze field differences with UNIVERSAL DATA TYPE SUPPORT
     */
    async analyzeCommonFieldDifferences(tempTableId, sourceTableName, primaryKey, commonFields, matchedIds) {
        try {
            console.log(`FIXED: Analyzing field differences with UNIVERSAL DATA TYPE SUPPORT...`);
            console.log(`Common fields available: ${commonFields.length}`);
            console.log(`Matched records to analyze: ${matchedIds.length}`);

            if (matchedIds.length === 0) {
                return {
                    totalFieldIssues: 0,
                    fieldComparison: [],
                    fieldsAnalyzed: 0,
                    recordsAnalyzed: 0,
                    perfectFields: 0,
                    problematicFields: 0,
                    summary: 'No matched records to analyze - 0 matches found'
                };
            }

            if (commonFields.length === 0) {
                return {
                    totalFieldIssues: 0,
                    fieldComparison: [],
                    fieldsAnalyzed: 0,
                    recordsAnalyzed: 0,
                    perfectFields: 0,
                    problematicFields: 0,
                    summary: 'No common fields found for comparison'
                };
            }

            const fieldComparison = [];
            let totalFieldIssues = 0;

            const safeFields = commonFields.filter(field => {
                const lowerField = field.toLowerCase();
                return field !== primaryKey &&
                       !lowerField.includes('comment') &&
                       !lowerField.includes('description') &&
                       !lowerField.includes('sys_domain_path') &&
                       !lowerField.includes('sys_tags') &&
                       !lowerField.includes('header') &&
                       field.length < 50;
            }).slice(0, 10);

            console.log(`Safe fields to analyze with universal data type support: [${safeFields.join(', ')}]`);

            for (const field of safeFields) {
                try {
                    console.log(`FIXED: Analyzing field: ${field} with universal data type casting for ${matchedIds.length} matched records...`);

                    // FIXED: Get data types for this field
                    const fieldDataTypes = await this.getFieldDataTypes(tempTableId, sourceTableName, field);
                    const commonType = this.getBestCommonType(fieldDataTypes.tempType, fieldDataTypes.sourceType);

                    console.log(`Field ${field}: JSON type=${fieldDataTypes.tempType}, BQ type=${fieldDataTypes.sourceType}, common type=${commonType}`);

                    const matchedIdsStr = matchedIds.slice(0, 50).map(id => `'${String(id).replace(/'/g, "\\'")}'`).join(',');

                    // Get primary key data types for JOIN
                    const pkDataTypes = await this.getFieldDataTypes(tempTableId, sourceTableName, primaryKey);
                    const pkCommonType = this.getBestCommonType(pkDataTypes.tempType, pkDataTypes.sourceType);

                    // Create cast expressions
                    const tempFieldCast = this.getCastExpression(`json_table.${field}`, fieldDataTypes.tempType, commonType);
                    const sourceFieldCast = this.getCastExpression(`bq_table.${field}`, fieldDataTypes.sourceType, commonType);
                    const tempPkCast = this.getCastExpression(`json_table.${primaryKey}`, pkDataTypes.tempType, pkCommonType);
                    const sourcePkCast = this.getCastExpression(`bq_table.${primaryKey}`, pkDataTypes.sourceType, pkCommonType);

                    // FIXED: Field comparison query with proper NULL handling
                    // NULL to NULL is treated as MATCH, not a mismatch
                    const fieldComparisonQuery = `
                        SELECT
                            ${tempPkCast} as record_key,
                            ${tempFieldCast} as json_value,
                            ${sourceFieldCast} as bq_value,
                            CASE
                                -- Both NULL = MATCH
                                WHEN json_table.${field} IS NULL AND bq_table.${field} IS NULL THEN 'MATCH'
                                -- One NULL, one not = DIFFER
                                WHEN json_table.${field} IS NULL OR bq_table.${field} IS NULL THEN 'DIFFER'
                                -- Both not NULL, compare casted values
                                WHEN ${tempFieldCast} = ${sourceFieldCast} THEN 'MATCH'
                                ELSE 'DIFFER'
                            END as comparison_result
                        FROM \`${tempTableId}\` json_table
                        INNER JOIN \`${sourceTableName}\` bq_table
                        ON ${tempPkCast} = ${sourcePkCast}
                        WHERE ${tempPkCast} IN (${matchedIdsStr})
                        LIMIT 100
                    `;

                    const [fieldResult] = await this.bigquery.query(fieldComparisonQuery);

                    const differences = fieldResult.filter(r => r.comparison_result === 'DIFFER');
                    const matches = fieldResult.filter(r => r.comparison_result === 'MATCH');

                    console.log(`FIXED: Field ${field} with universal casting: ${matches.length} matches, ${differences.length} differences`);

                    // For sample matches, prefer records with actual values (not both NULL)
                    const matchesWithValues = matches.filter(m => m.json_value !== null && m.json_value !== 'null' && m.json_value !== '');
                    const sampleMatchesToShow = matchesWithValues.length > 0 ? matchesWithValues : matches;

                    console.log(`Field ${field}: ${sampleMatchesToShow.length} sample matches to show`);

                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: fieldResult.length,
                        perfectMatches: matches.length,
                        differences: differences.length,
                        matchRate: fieldResult.length > 0 ? ((matches.length / fieldResult.length) * 100).toFixed(1) : '0.0',
                        sampleDifferences: differences.slice(0, 20).map(d => ({
                            primaryKey: d.record_key,
                            apiValue: d.json_value,
                            bqValue: d.bq_value
                        })),
                        sampleMatches: sampleMatchesToShow.slice(0, 10).map(m => ({
                            primaryKey: m.record_key,
                            value: m.json_value
                        })),
                        allComparisons: fieldResult.slice(0, 10),
                        dataTypes: {
                            jsonType: fieldDataTypes.tempType,
                            bqType: fieldDataTypes.sourceType,
                            commonType: commonType
                        }
                    });

                    totalFieldIssues += differences.length;

                } catch (fieldError) {
                    console.warn(`FIXED: Field ${field} analysis failed, but now with graceful error handling:`, fieldError.message);

                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: 0,
                        perfectMatches: 0,
                        differences: 0,
                        matchRate: '0.0',
                        error: `Data type casting failed: ${fieldError.message}`,
                        sampleDifferences: [],
                        sampleMatches: [],
                        allComparisons: [],
                        dataTypes: {
                            jsonType: 'UNKNOWN',
                            bqType: 'UNKNOWN',
                            commonType: 'STRING'
                        }
                    });
                }
            }

            console.log(`FIXED: Field analysis with universal data types completed: ${totalFieldIssues} total field issues found across ${safeFields.length} fields`);

            return {
                totalFieldIssues: totalFieldIssues,
                fieldComparison: fieldComparison,
                fieldsAnalyzed: safeFields.length,
                recordsAnalyzed: matchedIds.length,
                perfectFields: fieldComparison.filter(f => f.differences === 0 && !f.error).length,
                problematicFields: fieldComparison.filter(f => f.differences > 0 || f.error).length,
                summary: `FIXED: Analyzed ${safeFields.length} common fields with universal data type support across ${matchedIds.length} matched records`
            };

        } catch (error) {
            console.error('FIXED: Universal data type field analysis failed:', error.message);
            return {
                totalFieldIssues: 0,
                fieldComparison: [],
                fieldsAnalyzed: 0,
                recordsAnalyzed: 0,
                perfectFields: 0,
                problematicFields: 0,
                summary: 'FIXED: Field analysis failed: ' + error.message
            };
        }
    }

    /**
     * MAIN: Schema-safe comparison using any common field with universal data type support
     */
    async compareJSONvsBigQuery(tempTableId, sourceTableName, primaryKey = 'Id', comparisonFields = [], strategy = 'enhanced') {
        try {
            console.log('Starting comparison...');
            console.log(`SOURCE (JSON): ${tempTableId}`);
            console.log(`TARGET (BigQuery): ${sourceTableName}`);
            console.log(`Requested primary key: ${primaryKey}`);

            // STEP 1: Get common fields analysis
            const schemaAnalysis = await this.getCommonFields(tempTableId, sourceTableName);
            console.log(`Schema analysis completed: ${schemaAnalysis.commonFields.length} common fields found`);

            // STEP 2: Validate the requested primary key exists in both tables
            if (!schemaAnalysis.commonFields.includes(primaryKey)) {
                console.error(`Primary key '${primaryKey}' not found in common fields!`);
                const suggestedKey = schemaAnalysis.primaryKeyCandidates[0] || schemaAnalysis.commonFields[0];
                throw new Error(`Primary key '${primaryKey}' not available in both tables. Suggested alternative: '${suggestedKey}'. Available common fields: ${schemaAnalysis.commonFields.slice(0, 5).join(', ')}`);
            }

            console.log(`Primary key '${primaryKey}' found in common fields - proceeding with comparison`);

            // STEP 3: Validate the primary key field works in both tables
            const keyValidation = await this.validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, schemaAnalysis.commonFields);
            console.log(`Primary key '${primaryKey}' validated in both tables`);

            // STEP 4: Get record counts using validated primary key
            const recordCounts = await this.getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey);

            // STEP 5: Find matching records
            const matchAnalysis = await this.getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey);

            // STEP 6: Analyze field differences for common fields with UNIVERSAL DATA TYPE SUPPORT
            const fieldAnalysis = await this.analyzeCommonFieldDifferences(
                tempTableId,
                sourceTableName,
                primaryKey,
                schemaAnalysis.commonFields,
                matchAnalysis.matchedIds
            );

            // STEP 7: Get comprehensive duplicates analysis (both systems)
            const duplicatesAnalysis = await this.analyzeBothSystemDuplicates(tempTableId, sourceTableName, primaryKey);

            // STEP 8: Create comprehensive results
            const summary = {
                totalRecordsInFile: recordCounts.jsonDetails.totalRecords,
                uniqueSourceRecords: recordCounts.jsonDetails.uniquePrimaryKeys,
                duplicateRecordsInFile: recordCounts.jsonDetails.duplicateRecords,
                targetRecords: recordCounts.bqDetails.totalRecords,
                recordsReachedTarget: matchAnalysis.matchCount,
                recordsFailedToReachTarget: matchAnalysis.jsonOnlyCount,
                recordsOnlyInTarget: matchAnalysis.bqOnlyCount,
                pipelineSuccessRate: recordCounts.jsonDetails.uniquePrimaryKeys > 0 ?
                    ((matchAnalysis.matchCount / recordCounts.jsonDetails.uniquePrimaryKeys) * 100).toFixed(1) : '0.0',
                matchRate: recordCounts.jsonDetails.totalRecords > 0 ?
                    ((matchAnalysis.matchCount / recordCounts.jsonDetails.totalRecords) * 100).toFixed(1) : '0.0',
                fieldsAnalyzed: fieldAnalysis.fieldsAnalyzed,
                totalFieldIssues: fieldAnalysis.totalFieldIssues,
                schemaCompatibility: (schemaAnalysis.schemaCompatibility * 100).toFixed(1) + '%',
                commonFieldsCount: schemaAnalysis.commonFields.length,
                primaryKeyUsed: primaryKey,
                matchedRecordIds: matchAnalysis.matchedIds,
                failedRecordIds: matchAnalysis.jsonOnlyIds,
                comparisonDate: new Date().toISOString(),
                strategy: 'enhanced-universal-data-types'
            };

            console.log('Comparison completed successfully');
            console.log(`Results: ${matchAnalysis.matchCount} matches found using '${primaryKey}' field`);
            console.log(`Pipeline success rate: ${summary.pipelineSuccessRate}%`);

            return {
                success: true,
                analysisType: 'enhanced-universal-data-types',
                primaryKeyUsed: primaryKey,
                schemaAnalysis: schemaAnalysis,
                recordCounts: recordCounts,
                comparisonResults: {
                    matches: {
                        matchCount: matchAnalysis.matchCount,
                        matchedIds: matchAnalysis.matchedIds,
                        sampleMatches: matchAnalysis.sampleMatches
                    },
                    missing: {
                        missingFromBQ: matchAnalysis.jsonOnlyRecords,
                        missingFromJSON: matchAnalysis.bqOnlyRecords
                    },
                    fieldDifferences: fieldAnalysis,
                    dataTypes: matchAnalysis.dataTypes
                },
                fieldWiseAnalysis: fieldAnalysis,
                duplicatesAnalysis: duplicatesAnalysis,
                summary: summary,
                metadata: {
                    tempTableId,
                    sourceTableName,
                    primaryKey: primaryKey,
                    comparisonFields: comparisonFields.length > 0 ? comparisonFields : schemaAnalysis.commonFields.slice(0, 10),
                    strategy: 'enhanced-universal-data-types',
                    comparisonDate: new Date().toISOString(),
                    dataTypeSupport: 'Enhanced (all BigQuery types with automatic casting)',
                    duplicateAnalysis: 'Dual-system (JSON + BigQuery)'
                }
            };

        } catch (error) {
            console.error('Comparison failed:', error.message);
            throw new Error(`Comparison failed: ${error.message}`);
        }
    }


     async compareJSONvsBigQueryWithFilter(tempTableId, sourceTable, primaryKey, comparisonFields = [], strategy = 'enhanced', bqFilter = null, unnestField = null) {
    try {
        console.log('=== ENHANCED COMPARISON WITH BIGQUERY FILTERING (FIXED VERSION) ===');
        console.log(`Temp table: ${tempTableId}`);
        console.log(`Source table: ${sourceTable}`);
        console.log(`Primary key: ${primaryKey}`);
        console.log(`Raw BigQuery filter: "${bqFilter}"`);
        console.log(`Unnest field: "${unnestField || 'none'}"`);

        // FIXED: Preprocess and validate the filter
        let processedFilter = null;

        if (bqFilter) {
            try {
                processedFilter = this.preprocessFilter(bqFilter);
                console.log(`Processed filter: "${processedFilter}"`);
            } catch (preprocessError) {
                throw new Error(`Filter preprocessing failed: ${preprocessError.message}`);
            }
        }

        if (processedFilter) {
            console.log('Validating BigQuery filter...');
            const filterValidation = await this.validateFilterAgainstTable(sourceTable, processedFilter);

            if (!filterValidation.isValid) {
                throw new Error(`Filter validation failed: ${filterValidation.error}${filterValidation.suggestion ? '\nSuggestion: ' + filterValidation.suggestion : ''}`);
            }

            console.log('BigQuery filter validation passed');

            if (filterValidation.warning) {
                console.warn('Filter validation warning:', filterValidation.warning);
            }
        }

        // FIXED: Build the filtered source table query with proper error handling
        // NEW: Pass unnestField for nested array support
        const filteredSourceQuery = this.buildFilteredSourceQuery(sourceTable, processedFilter, unnestField);
        console.log(`Filtered source query: ${filteredSourceQuery}`);

        // Run the comparison using the filtered source
        const results = await this.executeFilteredComparison(
            tempTableId,
            filteredSourceQuery,
            sourceTable, // Keep original table name for metadata
            primaryKey,
            comparisonFields,
            strategy,
            processedFilter,
            unnestField // NEW: Pass unnest field
        );

        // Add comprehensive filter information to results
        results.filterInformation = {
            applied: !!processedFilter,
            rawCondition: bqFilter,
            processedCondition: processedFilter,
            originalTable: sourceTable,
            description: processedFilter
                ? `Applied filter: ${processedFilter}`
                : 'No filtering applied - compared all BigQuery records',
            validation: processedFilter ? {
                passed: true,
                recordsMatched: results.recordCounts?.bqDetails?.totalRecords || 0,
                originalRecords: results.recordCounts?.originalBqCounts?.originalTotal || null
            } : null
        };

        return results;

    } catch (error) {
        console.error('Enhanced comparison with BigQuery filtering failed:', error.message);

        // FIXED: Return structured error with helpful information
        return {
            success: false,
            error: error.message,
            filterInformation: {
                applied: false,
                rawCondition: bqFilter,
                processedCondition: null,
                originalTable: sourceTable,
                description: 'Filter validation or application failed',
                validation: {
                    passed: false,
                    error: error.message
                }
            },
            metadata: {
                tempTable: tempTableId,
                sourceTable: sourceTable,
                primaryKey: primaryKey,
                timestamp: new Date().toISOString(),
                errorType: 'filter_validation_error'
            }
        };
    }
}

buildFilteredSourceQuery(sourceTable, bqFilter, unnestField = null) {
    // NOTE: unnestField is used for API-side explosion only
    // BigQuery tables are typically already flattened/unnested, so we don't apply UNNEST here
    // The unnestField parameter is kept for metadata purposes but not used in the query
    
    if (unnestField && unnestField.trim()) {
        console.log(`Note: API data was exploded using field '${unnestField}' - BigQuery table assumed to be already flattened`);
    }
    
    if (!bqFilter || !bqFilter.trim()) {
        // No filter - return original table reference
        return `\`${sourceTable}\``;
    }

    // Create a subquery with the filter applied AND an alias
    const filteredQuery = `(SELECT * FROM \`${sourceTable}\` WHERE ${bqFilter.trim()}) AS filtered_bq_table`;

    return filteredQuery;
}

async validateFilterAgainstTable(sourceTable, filterCondition) {
    try {
        console.log(`Validating filter against table: ${sourceTable}`);
        console.log(`Filter condition: "${filterCondition}"`);

        // FIXED: Pre-validation checks for empty or problematic filters
        if (!filterCondition || !filterCondition.trim()) {
            return {
                isValid: false,
                error: 'Filter condition cannot be empty. Please provide a valid WHERE clause condition.',
                suggestion: 'Example: account_id = \'value\' OR created_date > \'2024-01-01\''
            };
        }

        const trimmedFilter = filterCondition.trim();

        // FIXED: Basic syntax validation before attempting BigQuery execution
        if (trimmedFilter.toLowerCase().startsWith('where')) {
            return {
                isValid: false,
                error: 'Filter condition should not include the WHERE keyword. Provide only the condition.',
                suggestion: `Instead of "WHERE ${trimmedFilter}", use: ${trimmedFilter.substring(5).trim()}`
            };
        }

        // Check for obviously problematic patterns
        if (trimmedFilter.includes(';;') || trimmedFilter.includes('--') || trimmedFilter.match(/['"]\s*['"]/)) {
            return {
                isValid: false,
                error: 'Filter contains potentially problematic syntax. Please check for typos or invalid SQL.',
                suggestion: 'Use standard SQL syntax: field_name = \'value\' AND other_field > 100'
            };
        }

        // FIXED: Test the filter with a LIMIT 0 query to validate syntax without returning data
        const testQuery = `
            SELECT *
            FROM \`${sourceTable}\`
            WHERE ${trimmedFilter}
            LIMIT 0
        `;

        console.log('Executing filter validation query...');
        const [testResult] = await this.bigquery.query(testQuery);
        console.log('Filter validation query executed successfully');

        // FIXED: Also test that the filter returns some records (not empty result)
        const countQuery = `
            SELECT COUNT(*) as filtered_count
            FROM \`${sourceTable}\`
            WHERE ${trimmedFilter}
        `;

        console.log('Checking filter result count...');
        const [countResult] = await this.bigquery.query(countQuery);
        const filteredCount = parseInt(countResult[0]?.filtered_count || 0);

        console.log(`Filter validation: ${filteredCount} records match the filter condition`);

        if (filteredCount === 0) {
            console.warn('Filter condition returns no records - this may not be intended');
            return {
                isValid: true,
                warning: `Filter condition "${trimmedFilter}" returns 0 records from ${sourceTable}`,
                filteredCount: 0,
                message: 'Filter is syntactically valid but matches no records'
            };
        }

        return {
            isValid: true,
            filteredCount: filteredCount,
            message: `Filter validated successfully - ${filteredCount} records match the condition "${trimmedFilter}"`
        };

    } catch (error) {
        console.error('Filter validation failed:', error.message);

        // FIXED: Enhanced error parsing to provide better feedback
        const errorMessage = error.message || '';

        if (errorMessage.includes('Unrecognized name')) {
            const fieldMatch = errorMessage.match(/Unrecognized name: (\w+)/);
            const fieldName = fieldMatch ? fieldMatch[1] : 'unknown';
            return {
                isValid: false,
                error: `Field not found: '${fieldName}' does not exist in table ${sourceTable}`,
                fieldName: fieldName,
                suggestion: 'Check the table schema for available field names'
            };
        } else if (errorMessage.includes('Syntax error')) {
            return {
                isValid: false,
                error: `Invalid SQL syntax in filter condition: "${filterCondition}"`,
                suggestion: 'Use standard SQL WHERE clause syntax. Examples:\n- field_name = \'value\'\n- numeric_field > 100\n- date_field >= \'2024-01-01\'\n- field_name IN (\'val1\', \'val2\')'
            };
        } else if (errorMessage.includes('Invalid empty identifier')) {
            return {
                isValid: false,
                error: 'Filter contains empty or invalid identifiers. Check for typos or missing values.',
                suggestion: 'Ensure all field names and values are properly specified'
            };
        } else if (errorMessage.includes('Expected')) {
            return {
                isValid: false,
                error: `SQL parsing error: ${errorMessage}`,
                suggestion: 'Check for missing quotes, parentheses, or operators in your filter condition'
            };
        } else {
            return {
                isValid: false,
                error: `Filter validation error: ${errorMessage}`,
                suggestion: 'Please check your filter syntax and try again'
            };
        }
    }
}

/**
 * FIXED: Enhanced filter preprocessing
 */
preprocessFilter(filterCondition) {
    if (!filterCondition || !filterCondition.trim()) {
        return null;
    }

    let processed = filterCondition.trim();

    // Remove WHERE keyword if present
    if (processed.toLowerCase().startsWith('where ')) {
        processed = processed.substring(6).trim();
    }

    // Basic validation
    if (!processed) {
        throw new Error('Filter condition is empty after preprocessing');
    }

    return processed;
}

    async executeFilteredComparison(tempTableId, filteredSourceQuery, originalSourceTable, primaryKey, comparisonFields, strategy, bqFilter, unnestField = null) {
        try {
            console.log('Executing filtered comparison...');
            if (unnestField) {
                console.log(`UNNEST mode enabled for field: ${unnestField}`);
            }

            // Get record counts with filtering
            const recordCounts = await this.getFilteredRecordCounts(tempTableId, filteredSourceQuery, originalSourceTable, primaryKey, bqFilter, unnestField);

            // Get schema analysis with filtering
            const schemaAnalysis = await this.getFilteredSchemaAnalysis(tempTableId, filteredSourceQuery, originalSourceTable);

            // Run field-wise comparison with filtering
            const fieldWiseAnalysis = await this.getFilteredFieldWiseAnalysis(tempTableId, filteredSourceQuery, primaryKey, comparisonFields);

            // Run duplicates analysis with filtering
            const duplicatesAnalysis = await this.getFilteredDuplicatesAnalysis(tempTableId, filteredSourceQuery, primaryKey);
            
            // NEW: Run missing records analysis with filtering
            const missingRecordsAnalysis = await this.getFilteredMissingRecordsAnalysis(tempTableId, filteredSourceQuery, primaryKey);

            // Generate comprehensive summary
            const summary = this.generateFilteredSummary(recordCounts, schemaAnalysis, fieldWiseAnalysis, duplicatesAnalysis, bqFilter);

            return {
                success: true,
                recordCounts: recordCounts,
                schemaAnalysis: schemaAnalysis,
                comparisonResults: {
                    strategy: strategy,
                    primaryKeyUsed: primaryKey,
                    filterApplied: !!(bqFilter && bqFilter.trim()),
                    filterCondition: bqFilter && bqFilter.trim() ? bqFilter.trim() : null,
                    unnestApplied: !!(unnestField && unnestField.trim()),
                    unnestField: unnestField && unnestField.trim() ? unnestField.trim() : null
                },
                fieldWiseAnalysis: fieldWiseAnalysis,
                duplicatesAnalysis: duplicatesAnalysis,
                missingRecordsAnalysis: missingRecordsAnalysis, // NEW: Include missing records
                summary: summary,
                metadata: {
                    tempTable: tempTableId,
                    sourceTable: originalSourceTable,
                    filteredSource: bqFilter && bqFilter.trim() ? true : false,
                    unnestApplied: unnestField && unnestField.trim() ? true : false,
                    primaryKey: primaryKey,
                    timestamp: new Date().toISOString()
                }
            };

        } catch (error) {
            console.error('Filtered comparison execution failed:', error.message);
            throw error;
        }
    }

    /**
     * Get record counts with BigQuery filtering applied
     */
    async getFilteredRecordCounts(tempTableId, filteredSourceQuery, originalSourceTable, primaryKey, bqFilter, unnestField = null) {
        try {
            console.log('Getting filtered record counts...');
            
            // Note: unnestField is for API-side explosion only
            // BigQuery tables are assumed to be already flattened
            if (unnestField && unnestField.trim()) {
                console.log(`Note: API data was exploded using '${unnestField}' - BigQuery table assumed to be already flat`);
            }

            const queries = {
                // API/JSON temp table counts (unchanged)
                tempTotal: `SELECT COUNT(*) as count FROM \`${tempTableId}\``,
                tempUnique: `SELECT COUNT(DISTINCT CAST(${primaryKey} AS STRING)) as count FROM \`${tempTableId}\` WHERE ${primaryKey} IS NOT NULL`,
                tempNulls: `SELECT COUNT(*) as count FROM \`${tempTableId}\` WHERE ${primaryKey} IS NULL`,
                tempDuplicates: `
                    SELECT COUNT(*) as count FROM (
                        SELECT CAST(${primaryKey} AS STRING) as key_val
                        FROM \`${tempTableId}\`
                        WHERE ${primaryKey} IS NOT NULL
                        GROUP BY CAST(${primaryKey} AS STRING)
                        HAVING COUNT(*) > 1
                    )
                `,

                // BigQuery source counts (with filtering applied)
                sourceTotal: `SELECT COUNT(*) as count FROM ${filteredSourceQuery}`,
                sourceUnique: `SELECT COUNT(DISTINCT CAST(${primaryKey} AS STRING)) as count FROM ${filteredSourceQuery} WHERE ${primaryKey} IS NOT NULL`,
                sourceNulls: `SELECT COUNT(*) as count FROM ${filteredSourceQuery} WHERE ${primaryKey} IS NULL`,
                sourceDuplicates: `
                    SELECT COUNT(*) as count FROM (
                        SELECT CAST(${primaryKey} AS STRING) as key_val
                        FROM ${filteredSourceQuery}
                        WHERE ${primaryKey} IS NOT NULL
                        GROUP BY CAST(${primaryKey} AS STRING)
                        HAVING COUNT(*) > 1
                    )
                `
            };

            // If filter applied, also get original table counts for comparison
            let originalCounts = null;
            if (bqFilter && bqFilter.trim()) {
                console.log('Getting original (unfiltered) counts for comparison...');
                
                const originalQueries = {
                    originalTotal: `SELECT COUNT(*) as count FROM \`${originalSourceTable}\``,
                    originalUnique: `SELECT COUNT(DISTINCT CAST(${primaryKey} AS STRING)) as count FROM \`${originalSourceTable}\` WHERE ${primaryKey} IS NOT NULL`
                };

                originalCounts = {};
                for (const [key, query] of Object.entries(originalQueries)) {
                    try {
                        const [result] = await this.bigquery.query(query);
                        originalCounts[key] = parseInt(result[0]?.count || 0);
                        console.log(`${key}: ${originalCounts[key]}`);
                    } catch (error) {
                        console.warn(`Original count query '${key}' failed: ${error.message}`);
                        originalCounts[key] = 0;
                    }
                }
            }

            const results = {};
            for (const [key, query] of Object.entries(queries)) {
                try {
                    const [result] = await this.bigquery.query(query);
                    results[key] = parseInt(result[0]?.count || 0);
                } catch (error) {
                    console.warn(`Count query '${key}' failed: ${error.message}`);
                    results[key] = 0;
                }
            }

            return {
                jsonDetails: {
                    totalRecords: results.tempTotal,
                    uniquePrimaryKeys: results.tempUnique,
                    nullPrimaryKeys: results.tempNulls,
                    duplicateRecords: results.tempDuplicates,
                    primaryKeyField: primaryKey
                },
                bqDetails: {
                    totalRecords: results.sourceTotal,
                    uniquePrimaryKeys: results.sourceUnique,
                    nullPrimaryKeys: results.sourceNulls,
                    duplicateRecords: results.sourceDuplicates,
                    primaryKeyField: primaryKey,
                    filterApplied: !!(bqFilter && bqFilter.trim()),
                    filterCondition: bqFilter && bqFilter.trim() ? bqFilter.trim() : null
                },
                originalBqCounts: originalCounts, // NEW: Include original counts if filter was applied
                filterSummary: bqFilter && bqFilter.trim() ? {
                    applied: true,
                    condition: bqFilter.trim(),
                    filteredRecords: results.sourceTotal,
                    originalRecords: originalCounts?.originalTotal || null,
                    reductionCount: originalCounts?.originalTotal ? (originalCounts.originalTotal - results.sourceTotal) : null,
                    reductionPercentage: originalCounts?.originalTotal && originalCounts.originalTotal > 0
                        ? ((originalCounts.originalTotal - results.sourceTotal) / originalCounts.originalTotal * 100).toFixed(1)
                        : null
                } : {
                    applied: false,
                    condition: null
                }
            };

        } catch (error) {
            console.error('Filtered record counts failed:', error.message);
            throw error;
        }
    }
   async getFilteredSchemaAnalysis(tempTableId, filteredSourceQuery, originalSourceTable) {
    try {
        console.log('FIXED: Getting schema analysis for filtered comparison...');
        console.log(`Using original table for schema analysis: ${originalSourceTable}`);

        // FIXED: Schema analysis should use the original table, not the filtered subquery
        // The field structure doesn't change with filtering, only the record count does
        return await this.getCommonFields(tempTableId, originalSourceTable);
    } catch (error) {
        console.error('Filtered schema analysis failed:', error.message);
        throw error;
    }
}
/**
 * FIXED: Enhanced getFilteredFieldWiseAnalysis with proper subquery handling
 * Replace your existing getFilteredFieldWiseAnalysis method with this version
 * UPDATED: NULL to NULL is now treated as a MATCH, not a mismatch
 */
async getFilteredFieldWiseAnalysis(tempTableId, filteredSourceQuery, primaryKey, comparisonFields) {
    try {
        console.log('Running filtered field-wise analysis...');

        // FIXED: Get schema from original table, not filtered subquery
        let originalTable;
        if (filteredSourceQuery.includes('SELECT') && filteredSourceQuery.includes('FROM `')) {
            const tableMatch = filteredSourceQuery.match(/FROM\s+`([^`]+)`/);
            originalTable = tableMatch ? tableMatch[1] : null;
        }

        if (!originalTable) {
            console.warn('Could not extract original table for schema analysis, using basic field analysis');
            return {
                fieldsAnalyzed: 0,
                fieldComparison: [],
                perfectFields: 0,
                problematicFields: 0,
                totalFieldIssues: 0,
                error: 'Could not determine original table for schema analysis'
            };
        }

        console.log(`Using original table for schema analysis: ${originalTable}`);
        const schemaInfo = await this.getCommonFields(tempTableId, originalTable);
        const fieldsToCompare = comparisonFields.length > 0 ? comparisonFields : schemaInfo.commonFields;

        if (fieldsToCompare.length === 0) {
            return {
                fieldsAnalyzed: 0,
                fieldComparison: [],
                perfectFields: 0,
                problematicFields: 0,
                totalFieldIssues: 0
            };
        }

        const fieldComparisons = [];
        let totalIssues = 0;
        let perfectFields = 0;
        let problematicFields = 0;

        // Extract filter condition from the filtered source query
        let filterCondition = null;
        if (filteredSourceQuery.includes('WHERE')) {
            const whereMatch = filteredSourceQuery.match(/WHERE\s+(.+?)\s*\)\s*AS/);
            if (whereMatch) {
                filterCondition = whereMatch[1];
                console.log(`Extracted filter condition: ${filterCondition}`);
            }
        }

        for (const fieldName of fieldsToCompare.slice(0, 10)) { // Limit to first 10 fields
            try {
                console.log(`Analyzing field: ${fieldName} with BigQuery filtering...`);

                // FIXED: Use proper CTE structure with filtered data
                // UPDATED: NULL to NULL is treated as MATCH using COALESCE or IS NOT DISTINCT FROM logic
                let comparisonQuery;

                if (filterCondition) {
                    // Use filtered BigQuery data
                    comparisonQuery = `
                        WITH filtered_bq_data AS (
                            SELECT *
                            FROM \`${originalTable}\`
                            WHERE ${filterCondition}
                        ),
                        comparison_data AS (
                            SELECT
                                CAST(t.${primaryKey} AS STRING) as record_key,
                                CAST(t.${fieldName} AS STRING) as json_value,
                                CAST(s.${fieldName} AS STRING) as bq_value,
                                CASE
                                    -- Both NULL = MATCH
                                    WHEN t.${fieldName} IS NULL AND s.${fieldName} IS NULL THEN 'MATCH'
                                    -- One NULL, one not = DIFFER
                                    WHEN t.${fieldName} IS NULL OR s.${fieldName} IS NULL THEN 'DIFFER'
                                    -- Both not NULL, compare values
                                    WHEN CAST(t.${fieldName} AS STRING) = CAST(s.${fieldName} AS STRING) THEN 'MATCH'
                                    ELSE 'DIFFER'
                                END as comparison_result
                            FROM \`${tempTableId}\` t
                            INNER JOIN filtered_bq_data s
                            ON CAST(t.${primaryKey} AS STRING) = CAST(s.${primaryKey} AS STRING)
                            WHERE t.${primaryKey} IS NOT NULL
                            AND s.${primaryKey} IS NOT NULL
                            LIMIT 1000
                        )
                        SELECT
                            COUNT(*) as total_records,
                            COUNTIF(comparison_result = 'MATCH') as perfect_matches,
                            COUNTIF(comparison_result = 'DIFFER') as differences,
                            ROUND(COUNTIF(comparison_result = 'MATCH') * 100.0 / NULLIF(COUNT(*), 0), 2) as match_rate
                        FROM comparison_data
                    `;
                } else {
                    // No filtering - compare all records
                    comparisonQuery = `
                        WITH comparison_data AS (
                            SELECT
                                CAST(t.${primaryKey} AS STRING) as record_key,
                                CAST(t.${fieldName} AS STRING) as json_value,
                                CAST(s.${fieldName} AS STRING) as bq_value,
                                CASE
                                    -- Both NULL = MATCH
                                    WHEN t.${fieldName} IS NULL AND s.${fieldName} IS NULL THEN 'MATCH'
                                    -- One NULL, one not = DIFFER
                                    WHEN t.${fieldName} IS NULL OR s.${fieldName} IS NULL THEN 'DIFFER'
                                    -- Both not NULL, compare values
                                    WHEN CAST(t.${fieldName} AS STRING) = CAST(s.${fieldName} AS STRING) THEN 'MATCH'
                                    ELSE 'DIFFER'
                                END as comparison_result
                            FROM \`${tempTableId}\` t
                            INNER JOIN \`${originalTable}\` s
                            ON CAST(t.${primaryKey} AS STRING) = CAST(s.${primaryKey} AS STRING)
                            WHERE t.${primaryKey} IS NOT NULL
                            AND s.${primaryKey} IS NOT NULL
                            LIMIT 1000
                        )
                        SELECT
                            COUNT(*) as total_records,
                            COUNTIF(comparison_result = 'MATCH') as perfect_matches,
                            COUNTIF(comparison_result = 'DIFFER') as differences,
                            ROUND(COUNTIF(comparison_result = 'MATCH') * 100.0 / NULLIF(COUNT(*), 0), 2) as match_rate
                        FROM comparison_data
                    `;
                }

                const [compResult] = await this.bigquery.query(comparisonQuery);
                const stats = compResult[0] || {};

                const fieldComparison = {
                    fieldName: fieldName,
                    totalRecords: parseInt(stats.total_records || 0),
                    perfectMatches: parseInt(stats.perfect_matches || 0),
                    differences: parseInt(stats.differences || 0),
                    matchRate: parseFloat(stats.match_rate || 0),
                    filterApplied: !!filterCondition
                };

                // Get sample comparisons - ONLY show actual differences (not NULL to NULL)
                let sampleQuery;

                if (filterCondition) {
                    sampleQuery = `
                        WITH filtered_bq_data AS (
                            SELECT *
                            FROM \`${originalTable}\`
                            WHERE ${filterCondition}
                        )
                        SELECT
                            CAST(t.${primaryKey} AS STRING) as record_key,
                            CAST(t.${fieldName} AS STRING) as json_value,
                            CAST(s.${fieldName} AS STRING) as bq_value,
                            CASE
                                WHEN t.${fieldName} IS NULL AND s.${fieldName} IS NULL THEN 'MATCH'
                                WHEN t.${fieldName} IS NULL OR s.${fieldName} IS NULL THEN 'DIFFER'
                                WHEN CAST(t.${fieldName} AS STRING) = CAST(s.${fieldName} AS STRING) THEN 'MATCH'
                                ELSE 'DIFFER'
                            END as comparison_result
                        FROM \`${tempTableId}\` t
                        INNER JOIN filtered_bq_data s
                        ON CAST(t.${primaryKey} AS STRING) = CAST(s.${primaryKey} AS STRING)
                        WHERE t.${primaryKey} IS NOT NULL
                        AND s.${primaryKey} IS NOT NULL
                        ORDER BY comparison_result DESC
                        LIMIT 50
                    `;
                } else {
                    sampleQuery = `
                        SELECT
                            CAST(t.${primaryKey} AS STRING) as record_key,
                            CAST(t.${fieldName} AS STRING) as json_value,
                            CAST(s.${fieldName} AS STRING) as bq_value,
                            CASE
                                WHEN t.${fieldName} IS NULL AND s.${fieldName} IS NULL THEN 'MATCH'
                                WHEN t.${fieldName} IS NULL OR s.${fieldName} IS NULL THEN 'DIFFER'
                                WHEN CAST(t.${fieldName} AS STRING) = CAST(s.${fieldName} AS STRING) THEN 'MATCH'
                                ELSE 'DIFFER'
                            END as comparison_result
                        FROM \`${tempTableId}\` t
                        INNER JOIN \`${originalTable}\` s
                        ON CAST(t.${primaryKey} AS STRING) = CAST(s.${primaryKey} AS STRING)
                        WHERE t.${primaryKey} IS NOT NULL
                        AND s.${primaryKey} IS NOT NULL
                        ORDER BY comparison_result DESC
                        LIMIT 50
                    `;
                }

                const [sampleResult] = await this.bigquery.query(sampleQuery);
                
                // Separate matches and differences for samples
                // For matches, show records that have actual values (prefer non-null values for display)
                const allMatches = (sampleResult || []).filter(r => r.comparison_result === 'MATCH');
                const matchesWithValues = allMatches.filter(r => r.json_value !== null && r.json_value !== 'null' && r.json_value !== '');
                const sampleMatchesToUse = matchesWithValues.length > 0 ? matchesWithValues : allMatches;
                
                const sampleMatches = sampleMatchesToUse
                    .slice(0, 10)
                    .map(m => ({
                        primaryKey: m.record_key,
                        value: m.json_value
                    }));
                
                // For differences, only show actual value differences
                const sampleDifferences = (sampleResult || [])
                    .filter(r => r.comparison_result === 'DIFFER')
                    .slice(0, 20)
                    .map(d => ({
                        primaryKey: d.record_key,
                        apiValue: d.json_value,
                        bqValue: d.bq_value
                    }));
                
                console.log(`Field ${fieldName}: ${sampleMatches.length} sample matches, ${sampleDifferences.length} sample differences`);

                fieldComparison.sampleMatches = sampleMatches;
                fieldComparison.sampleDifferences = sampleDifferences;
                fieldComparison.allComparisons = sampleResult || [];

                fieldComparisons.push(fieldComparison);

                if (fieldComparison.differences === 0) {
                    perfectFields++;
                } else {
                    problematicFields++;
                    totalIssues += fieldComparison.differences;
                }

                console.log(`Field ${fieldName}: ${fieldComparison.perfectMatches} matches, ${fieldComparison.differences} differences`);

            } catch (fieldError) {
                console.warn(`Field analysis failed for ${fieldName}:`, fieldError.message);
                fieldComparisons.push({
                    fieldName: fieldName,
                    error: fieldError.message,
                    filterApplied: !!filterCondition
                });
                problematicFields++;
            }
        }

        console.log(`Filtered field-wise analysis completed: ${totalIssues} total issues found`);

        return {
            fieldsAnalyzed: fieldsToCompare.length,
            fieldComparison: fieldComparisons,
            fieldResults: fieldComparisons, // alias for UI compatibility
            perfectFields: perfectFields,
            problematicFields: problematicFields,
            totalFieldIssues: totalIssues,
            recordsAnalyzed: fieldComparisons.length > 0 ? fieldComparisons[0].totalRecords : 0,
            filterContext: filterCondition ?
                `BigQuery data filtered using: ${filterCondition}` :
                'No filtering applied - compared all BigQuery records'
        };

    } catch (error) {
        console.error('Filtered field-wise analysis failed:', error.message);
        throw error;
    }
}
    /**
     * Get duplicates analysis with BigQuery filtering applied
     */
    async getFilteredDuplicatesAnalysis(tempTableId, filteredSourceQuery, primaryKey) {
        try {
            console.log('Running filtered duplicates analysis...');
            console.log(`Primary key for duplicates check: ${primaryKey}`);

            // API/JSON duplicates - get both count and sample keys
            const jsonDuplicatesQuery = `
                SELECT
                    CAST(${primaryKey} AS STRING) as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY CAST(${primaryKey} AS STRING)
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 100
            `;

            // BigQuery duplicates (with filtering applied) - get both count and sample keys
            const bqDuplicatesQuery = `
                SELECT
                    CAST(${primaryKey} AS STRING) as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM ${filteredSourceQuery}
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY CAST(${primaryKey} AS STRING)
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 100
            `;

            console.log('JSON duplicates query:', jsonDuplicatesQuery);
            console.log('BQ duplicates query:', bqDuplicatesQuery);

            const [jsonDuplicatesResult] = await this.bigquery.query(jsonDuplicatesQuery);
            const [bqDuplicatesResult] = await this.bigquery.query(bqDuplicatesQuery);

            console.log(`JSON duplicates found: ${jsonDuplicatesResult.length}`);
            console.log(`BQ duplicates found: ${bqDuplicatesResult.length}`);

            // Calculate totals from detailed results
            const jsonDuplicateCount = jsonDuplicatesResult.length;
            const jsonTotalDuplicateRecords = jsonDuplicatesResult.reduce((sum, dup) => sum + parseInt(dup.occurrence_count) - 1, 0);
            const jsonSampleKeys = jsonDuplicatesResult.slice(0, 20).map(dup => `${dup.duplicate_key} (${dup.occurrence_count}x)`);

            const bqDuplicateCount = bqDuplicatesResult.length;
            const bqTotalDuplicateRecords = bqDuplicatesResult.reduce((sum, dup) => sum + parseInt(dup.occurrence_count) - 1, 0);
            const bqSampleKeys = bqDuplicatesResult.slice(0, 20).map(dup => `${dup.duplicate_key} (${dup.occurrence_count}x)`);

            const jsonDuplicates = {
                duplicateCount: jsonDuplicateCount,
                count: jsonDuplicateCount, // alias for UI compatibility
                totalDuplicateRecords: jsonTotalDuplicateRecords,
                sampleKeys: jsonSampleKeys,
                samples: jsonSampleKeys, // alias for UI compatibility
                duplicateKeys: jsonDuplicatesResult.map(dup => ({
                    key: dup.duplicate_key,
                    occurrences: parseInt(dup.occurrence_count)
                }))
            };

            const bqDuplicates = {
                duplicateCount: bqDuplicateCount,
                count: bqDuplicateCount, // alias for UI compatibility
                totalDuplicateRecords: bqTotalDuplicateRecords,
                sampleKeys: bqSampleKeys,
                samples: bqSampleKeys, // alias for UI compatibility
                filterApplied: true,
                duplicateKeys: bqDuplicatesResult.map(dup => ({
                    key: dup.duplicate_key,
                    occurrences: parseInt(dup.occurrence_count)
                }))
            };

            // Cross-system duplicate analysis (with filtered BigQuery data)
            const jsonDupKeys = new Set(jsonDuplicatesResult.map(d => d.duplicate_key));
            const bqDupKeys = new Set(bqDuplicatesResult.map(d => d.duplicate_key));
            const commonDuplicateKeys = [...jsonDupKeys].filter(k => bqDupKeys.has(k));

            const crossSystemAnalysis = {
                commonDuplicateKeys: commonDuplicateKeys.slice(0, 20),
                commonDuplicateCount: commonDuplicateKeys.length,
                jsonOnlyDuplicateKeys: [...jsonDupKeys].filter(k => !bqDupKeys.has(k)).slice(0, 20),
                bqOnlyDuplicateKeys: [...bqDupKeys].filter(k => !jsonDupKeys.has(k)).slice(0, 20),
                filterApplied: true
            };

            const bothSystemsClean = jsonDuplicateCount === 0 && bqDuplicateCount === 0;
            const dataQualityScore = bothSystemsClean ? 'Excellent' :
                                    (jsonDuplicateCount + bqDuplicateCount < 5) ? 'Good' : 'Needs Attention';

            console.log(`Duplicates summary - JSON: ${jsonDuplicateCount}, BQ: ${bqDuplicateCount}, Common: ${commonDuplicateKeys.length}`);

            return {
                jsonDuplicates: jsonDuplicates,
                bqDuplicates: bqDuplicates,
                crossSystemAnalysis: crossSystemAnalysis,
                summary: {
                    bothSystemsClean: bothSystemsClean,
                    dataQualityScore: dataQualityScore,
                    totalDuplicateKeys: jsonDuplicateCount + bqDuplicateCount,
                    totalDuplicateRecords: jsonTotalDuplicateRecords + bqTotalDuplicateRecords,
                    filterContext: 'BigQuery duplicates analysis performed on filtered data'
                }
            };

        } catch (error) {
            console.error('Filtered duplicates analysis failed:', error.message);
            throw error;
        }
    }

    /**
     * Get missing records analysis with BigQuery filtering applied
     * Shows which records exist in API but not in BQ, and vice versa
     */
    async getFilteredMissingRecordsAnalysis(tempTableId, filteredSourceQuery, primaryKey) {
        try {
            console.log('Running filtered missing records analysis...');

            // Records in API but NOT in BigQuery (filtered)
            const apiOnlyQuery = `
                SELECT DISTINCT CAST(${primaryKey} AS STRING) as missing_key
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                  AND CAST(${primaryKey} AS STRING) NOT IN (
                    SELECT DISTINCT CAST(${primaryKey} AS STRING)
                    FROM ${filteredSourceQuery}
                    WHERE ${primaryKey} IS NOT NULL
                  )
                ORDER BY missing_key
                LIMIT 100
            `;

            // Records in BigQuery (filtered) but NOT in API
            const bqOnlyQuery = `
                SELECT DISTINCT CAST(${primaryKey} AS STRING) as missing_key
                FROM ${filteredSourceQuery}
                WHERE ${primaryKey} IS NOT NULL
                  AND CAST(${primaryKey} AS STRING) NOT IN (
                    SELECT DISTINCT CAST(${primaryKey} AS STRING)
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IS NOT NULL
                  )
                ORDER BY missing_key
                LIMIT 100
            `;

            // Count queries for totals
            const apiOnlyCountQuery = `
                SELECT COUNT(DISTINCT CAST(${primaryKey} AS STRING)) as count
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                  AND CAST(${primaryKey} AS STRING) NOT IN (
                    SELECT DISTINCT CAST(${primaryKey} AS STRING)
                    FROM ${filteredSourceQuery}
                    WHERE ${primaryKey} IS NOT NULL
                  )
            `;

            const bqOnlyCountQuery = `
                SELECT COUNT(DISTINCT CAST(${primaryKey} AS STRING)) as count
                FROM ${filteredSourceQuery}
                WHERE ${primaryKey} IS NOT NULL
                  AND CAST(${primaryKey} AS STRING) NOT IN (
                    SELECT DISTINCT CAST(${primaryKey} AS STRING)
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IS NOT NULL
                  )
            `;

            // Execute all queries
            const [apiOnlyResult, bqOnlyResult, apiOnlyCountResult, bqOnlyCountResult] = await Promise.all([
                this.bigquery.query(apiOnlyQuery).catch(e => { console.warn('API-only query failed:', e.message); return [[]]; }),
                this.bigquery.query(bqOnlyQuery).catch(e => { console.warn('BQ-only query failed:', e.message); return [[]]; }),
                this.bigquery.query(apiOnlyCountQuery).catch(e => { console.warn('API-only count failed:', e.message); return [[{count: 0}]]; }),
                this.bigquery.query(bqOnlyCountQuery).catch(e => { console.warn('BQ-only count failed:', e.message); return [[{count: 0}]]; })
            ]);

            const apiOnlyKeys = (apiOnlyResult[0] || []).map(r => r.missing_key);
            const bqOnlyKeys = (bqOnlyResult[0] || []).map(r => r.missing_key);
            const apiOnlyCount = parseInt(apiOnlyCountResult[0]?.[0]?.count || 0);
            const bqOnlyCount = parseInt(bqOnlyCountResult[0]?.[0]?.count || 0);

            console.log(`Missing records analysis: ${apiOnlyCount} in API only, ${bqOnlyCount} in BQ only`);

            return {
                apiOnlyRecords: {
                    count: apiOnlyCount,
                    sampleKeys: apiOnlyKeys,
                    description: 'Records that exist in API but NOT in BigQuery (after filter)',
                    truncated: apiOnlyCount > 100
                },
                bqOnlyRecords: {
                    count: bqOnlyCount,
                    sampleKeys: bqOnlyKeys,
                    description: 'Records that exist in BigQuery (after filter) but NOT in API',
                    truncated: bqOnlyCount > 100
                },
                summary: {
                    totalMismatches: apiOnlyCount + bqOnlyCount,
                    perfectMatch: apiOnlyCount === 0 && bqOnlyCount === 0,
                    primaryKeyUsed: primaryKey
                }
            };

        } catch (error) {
            console.error('Filtered missing records analysis failed:', error.message);
            return {
                apiOnlyRecords: { count: 0, sampleKeys: [], description: 'Analysis failed', error: error.message },
                bqOnlyRecords: { count: 0, sampleKeys: [], description: 'Analysis failed', error: error.message },
                summary: { totalMismatches: 0, perfectMatch: false, error: error.message }
            };
        }
    }

    /**
     * Generate comprehensive summary with filter information
     */
    generateFilteredSummary(recordCounts, schemaAnalysis, fieldWiseAnalysis, duplicatesAnalysis, bqFilter) {
        const totalRecordsInFile = recordCounts.jsonDetails?.totalRecords || 0;
        const uniqueSourceRecords = recordCounts.jsonDetails?.uniquePrimaryKeys || 0;
        const targetRecords = recordCounts.bqDetails?.totalRecords || 0;
        const originalTargetRecords = recordCounts.originalBqCounts?.originalTotal || null;

        // Calculate pipeline success rate
        const recordsReachedTarget = Math.min(uniqueSourceRecords, targetRecords);
        const recordsFailedToReachTarget = Math.max(0, uniqueSourceRecords - targetRecords);
        const pipelineSuccessRate = uniqueSourceRecords > 0
            ? ((recordsReachedTarget / uniqueSourceRecords) * 100).toFixed(1)
            : '0.0';

        return {
            // Core metrics
            totalRecordsInFile: totalRecordsInFile,
            uniqueSourceRecords: uniqueSourceRecords,
            targetRecords: targetRecords,
            recordsReachedTarget: recordsReachedTarget,
            recordsFailedToReachTarget: recordsFailedToReachTarget,
            pipelineSuccessRate: pipelineSuccessRate,

            // Schema compatibility
            commonFieldsCount: schemaAnalysis?.commonFields?.length || 0,
            schemaCompatibility: schemaAnalysis ?
                ((schemaAnalysis.commonFields.length / Math.max(schemaAnalysis.totalJsonFields, schemaAnalysis.totalBqFields)) * 100).toFixed(1) :
                '0.0',

            // Field analysis
            fieldsAnalyzed: fieldWiseAnalysis?.fieldsAnalyzed || 0,
            perfectFields: fieldWiseAnalysis?.perfectFields || 0,
            totalFieldIssues: fieldWiseAnalysis?.totalFieldIssues || 0,

            // Duplicates
            duplicateRecordsInFile: duplicatesAnalysis?.jsonDuplicates?.duplicateCount || 0,

            // NEW: Filter information
            bigQueryFilterApplied: !!(bqFilter && bqFilter.trim()),
            bigQueryFilterCondition: bqFilter && bqFilter.trim() ? bqFilter.trim() : null,
            originalBigQueryRecords: originalTargetRecords,
            filteredBigQueryRecords: targetRecords,
            filterReduction: originalTargetRecords && originalTargetRecords > targetRecords
                ? {
                    recordsRemoved: originalTargetRecords - targetRecords,
                    percentageReduced: ((originalTargetRecords - targetRecords) / originalTargetRecords * 100).toFixed(1)
                }
                : null,

            // Quality assessment
            overallQuality: pipelineSuccessRate >= 95 ? 'Excellent' :
                           pipelineSuccessRate >= 80 ? 'Good' :
                           pipelineSuccessRate >= 60 ? 'Fair' : 'Needs Attention'
        };
    }

    /**
     * LEGACY: Keep for backward compatibility
     */
    async analyzeDuplicates(tempTableId, primaryKey) {
        try {
            const duplicateKeysQuery = `
                SELECT
                    ${primaryKey} as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY ${primaryKey}
                HAVING COUNT(*) > 1
                ORDER BY occurrence_count DESC
            `;

            const [duplicateKeys] = await this.bigquery.query(duplicateKeysQuery);

            const hasDuplicates = duplicateKeys.length > 0;
            const totalDuplicateRecords = duplicateKeys.reduce((sum, dup) => sum + parseInt(dup.occurrence_count), 0) - duplicateKeys.length;

            const recommendations = [];
            if (hasDuplicates) {
                recommendations.push(`Review source data to understand why ${duplicateKeys.length} primary key values appear multiple times`);
                recommendations.push(`Consider using composite keys or additional fields for unique identification`);
                recommendations.push(`Data pipeline should include deduplication logic before loading to BigQuery`);
            } else {
                recommendations.push(`Perfect data quality - no duplicate primary keys found`);
            }

            return {
                hasDuplicates: hasDuplicates,
                duplicateCount: duplicateKeys.length,
                totalDuplicateRecords: totalDuplicateRecords,
                duplicateKeys: duplicateKeys.map(dup => ({
                    key: dup.duplicate_key,
                    count: parseInt(dup.occurrence_count)
                })),
                recommendations: recommendations
            };

        } catch (error) {
            console.error('Duplicates analysis failed:', error.message);
            return {
                hasDuplicates: false,
                duplicateCount: 0,
                totalDuplicateRecords: 0,
                duplicateKeys: [],
                recommendations: ['Duplicates analysis failed: ' + error.message]
            };
        }
    }

    // Legacy compatibility methods
    async getRecordCounts(tempTableId, sourceTableName) {
        try {
            const [jsonResult] = await this.bigquery.query(`SELECT COUNT(*) as count FROM \`${tempTableId}\``);
            const [bqResult] = await this.bigquery.query(`SELECT COUNT(*) as count FROM \`${sourceTableName}\``);

            return {
                jsonCount: parseInt(jsonResult[0].count),
                bqCount: parseInt(bqResult[0].count)
            };
        } catch (error) {
            console.error('Legacy getRecordCounts failed:', error.message);
            throw error;
        }
    }

    async findMatchingRecords(tempTableId, sourceTableName, primaryKey) {
        try {
            const matchQuery = `
                SELECT COUNT(DISTINCT json_table.${primaryKey}) as match_count
                FROM \`${tempTableId}\` json_table
                WHERE json_table.${primaryKey} IN (
                    SELECT DISTINCT ${primaryKey} FROM \`${sourceTableName}\` WHERE ${primaryKey} IS NOT NULL
                )
            `;

            const [matchResult] = await this.bigquery.query(matchQuery);
            return {
                matchCount: parseInt(matchResult[0].match_count),
                sampleMatches: []
            };
        } catch (error) {
            console.error('Legacy findMatchingRecords failed:', error.message);
            throw error;
        }
    }

    async findMissingRecords(tempTableId, sourceTableName, primaryKey) {
        try {
            const jsonOnlyQuery = `
                SELECT DISTINCT ${primaryKey}
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                  AND ${primaryKey} NOT IN (
                    SELECT DISTINCT ${primaryKey}
                    FROM \`${sourceTableName}\`
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;

            const bqOnlyQuery = `
                SELECT DISTINCT ${primaryKey}
                FROM \`${sourceTableName}\`
                WHERE ${primaryKey} IS NOT NULL
                  AND ${primaryKey} NOT IN (
                    SELECT DISTINCT ${primaryKey}
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;

            const [jsonOnlyResult, bqOnlyResult] = await Promise.all([
                this.bigquery.query(jsonOnlyQuery).catch(() => [[]]),
                this.bigquery.query(bqOnlyQuery).catch(() => [[]])
            ]);

            return {
                missingFromBQ: jsonOnlyResult[0] || [],
                missingFromJSON: bqOnlyResult[0] || []
            };
        } catch (error) {
            console.error('Legacy findMissingRecords failed:', error.message);
            throw error;
        }
    }

    async compareFieldDifferences(tempTableId, sourceTableName, primaryKey, comparisonFields = [], maxRecords = 100) {
        try {
            return {
                totalDifferences: 0,
                fieldDifferences: [],
                message: 'Using dynamic schema-safe common field analysis with universal data types'
            };
        } catch (error) {
            console.error('Legacy compareFieldDifferences failed:', error.message);
            throw error;
        }
    }

} // <-- END OF CLASS (all methods are now inside)

module.exports = ComparisonEngineService;