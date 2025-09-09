// services/comparison-engine.js - FIXED: SQL Ambiguity + Clean Version
const { BigQuery } = require('@google-cloud/bigquery');

class ComparisonEngineService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        
        console.log('Comparison Engine Service initialized - FIXED VERSION');
        console.log('FIXED: SQL table aliasing + data type support + dual duplicates analysis');
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
    async getFieldDataTypes(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`Getting data types for field: ${primaryKey}`);
            
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
                AND column_name = '${primaryKey}'
            `;
            
            const sourceTypeQuery = `
                SELECT 
                    column_name, 
                    data_type,
                    is_nullable
                FROM \`${sourceProject}\`.${sourceDataset}.INFORMATION_SCHEMA.COLUMNS 
                WHERE table_name = '${sourceTableName_clean}' 
                AND column_name = '${primaryKey}'
            `;

            let tempType = 'STRING';
            let sourceType = 'STRING';
            
            try {
                const [tempTypeResult] = await this.bigquery.query(tempTypeQuery);
                const [sourceTypeResult] = await this.bigquery.query(sourceTypeQuery);
                
                if (tempTypeResult.length > 0) tempType = tempTypeResult[0].data_type;
                if (sourceTypeResult.length > 0) sourceType = sourceTypeResult[0].data_type;
                
                console.log(`Data types detected - Temp: ${tempType}, Source: ${sourceType}`);
                
            } catch (typeError) {
                console.warn('Could not get schema info, using default STRING types:', typeError.message);
            }

            return { tempType, sourceType };
            
        } catch (error) {
            console.warn('Data type detection failed, using STRING fallback:', error.message);
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
    async getCommonFields(tempTableId, sourceTableName) {
        try {
            console.log('Starting field analysis...');
            console.log(`Temp table: ${tempTableId}`);
            console.log(`Source table: ${sourceTableName}`);
            
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
                const bqSchemaQuery = `SELECT * FROM \`${sourceTableName}\` LIMIT 1`;
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
     * FIXED: Find matches with proper table aliasing to avoid SQL ambiguity
     */
    async getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`Finding matches using primary key: ${primaryKey}`);

            const dataTypes = await this.getFieldDataTypes(tempTableId, sourceTableName, primaryKey);
            const commonType = this.getBestCommonType(dataTypes.tempType, dataTypes.sourceType);
            
            console.log(`Using common type for comparison: ${commonType}`);

            // FIXED: Proper table aliasing to avoid ambiguous column names
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

            const jsonDuplicateKeys = new Set(jsonDuplicates.map(dup => dup.duplicate_key));
            const bqDuplicateKeys = new Set(bqDuplicates.map(dup => dup.duplicate_key));
            
            const commonDuplicateKeys = [...jsonDuplicateKeys].filter(key => bqDuplicateKeys.has(key));
            const jsonOnlyDuplicateKeys = [...jsonDuplicateKeys].filter(key => !bqDuplicateKeys.has(key));
            const bqOnlyDuplicateKeys = [...bqDuplicateKeys].filter(key => !jsonDuplicateKeys.has(key));

            console.log(`Common duplicate keys: ${commonDuplicateKeys.length}`);

            let commonDuplicateDetails = [];
            if (commonDuplicateKeys.length > 0 && commonDuplicateKeys.length <= 10) {
                try {
                    const commonKeysStr = commonDuplicateKeys.map(key => `'${String(key).replace(/'/g, "\\'")}'`).join(',');
                    
                    const commonDuplicateDetailsQuery = `
                        SELECT 
                            'JSON' as source_system,
                            ${tempCast} as duplicate_key,
                            COUNT(*) as count_in_system
                        FROM \`${tempTableId}\`
                        WHERE ${tempCast} IN (${commonKeysStr})
                        GROUP BY ${tempCast}
                        
                        UNION ALL
                        
                        SELECT 
                            'BigQuery' as source_system,
                            ${sourceCast} as duplicate_key,
                            COUNT(*) as count_in_system
                        FROM \`${sourceTableName}\`
                        WHERE ${sourceCast} IN (${commonKeysStr})
                        GROUP BY ${sourceCast}
                        
                        ORDER BY duplicate_key, source_system
                    `;
                    
                    const [commonDetails] = await this.bigquery.query(commonDuplicateDetailsQuery);
                    commonDuplicateDetails = commonDetails;
                    
                } catch (detailsError) {
                    console.warn(`Could not get common duplicate details:`, detailsError.message);
                }
            }

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
                    totalDuplicateRecords: jsonDuplicateRecordCount,
                    duplicateKeys: jsonDuplicates.map(dup => ({
                        key: dup.duplicate_key,
                        count: parseInt(dup.occurrence_count)
                    }))
                },
                
                bqDuplicates: {
                    hasDuplicates: bqDuplicates.length > 0,
                    duplicateCount: bqDuplicates.length,
                    totalDuplicateRecords: bqDuplicateRecordCount,
                    duplicateKeys: bqDuplicates.map(dup => ({
                        key: dup.duplicate_key,
                        count: parseInt(dup.occurrence_count)
                    }))
                },
                
                crossSystemAnalysis: {
                    commonDuplicateKeys: commonDuplicateKeys,
                    jsonOnlyDuplicateKeys: jsonOnlyDuplicateKeys,
                    bqOnlyDuplicateKeys: bqOnlyDuplicateKeys,
                    commonDuplicateDetails: commonDuplicateDetails
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
                jsonDuplicates: { hasDuplicates: false, duplicateCount: 0, totalDuplicateRecords: 0, duplicateKeys: [] },
                bqDuplicates: { hasDuplicates: false, duplicateCount: 0, totalDuplicateRecords: 0, duplicateKeys: [] },
                crossSystemAnalysis: { commonDuplicateKeys: [], jsonOnlyDuplicateKeys: [], bqOnlyDuplicateKeys: [], commonDuplicateDetails: [] },
                summary: { totalSystemsWithDuplicates: 0, totalDuplicateKeys: 0, totalDuplicateRecords: 0, bothSystemsClean: true, criticalIssues: 0, dataQualityScore: 'Unknown' },
                recommendations: [`Duplicates analysis failed: ${error.message}`]
            };
        }
    }

    /**
     * MAIN: Schema-safe comparison using any common field with data type support
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

            // STEP 6: Analyze field differences for common fields only
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
                strategy: 'enhanced-data-type-support'
            };

            console.log('Comparison completed successfully');
            console.log(`Results: ${matchAnalysis.matchCount} matches found using '${primaryKey}' field`);
            console.log(`Pipeline success rate: ${summary.pipelineSuccessRate}%`);

            return {
                success: true,
                analysisType: 'enhanced-data-type-support',
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
                    strategy: 'enhanced-data-type-support',
                    comparisonDate: new Date().toISOString(),
                    dataTypeSupport: 'Enhanced (all BigQuery types)',
                    duplicateAnalysis: 'Dual-system (JSON + BigQuery)'
                }
            };

        } catch (error) {
            console.error('Comparison failed:', error.message);
            throw new Error(`Comparison failed: ${error.message}`);
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
     * Analyze field differences only for common fields
     */
    async analyzeCommonFieldDifferences(tempTableId, sourceTableName, primaryKey, commonFields, matchedIds) {
        try {
            console.log(`Analyzing field differences for common fields...`);
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
            
            console.log(`Safe fields to analyze: [${safeFields.join(', ')}]`);
            
            for (const field of safeFields) {
                try {
                    console.log(`Analyzing field: ${field} for ${matchedIds.length} matched records...`);
                    
                    const matchedIdsStr = matchedIds.slice(0, 50).map(id => `'${String(id).replace(/'/g, "\\'")}'`).join(',');
                    
                    // FIXED: Proper table aliasing to avoid ambiguous column names
                    const fieldComparisonQuery = `
                        SELECT 
                            json_table.${primaryKey} as record_key,
                            COALESCE(CAST(json_table.${field} AS STRING), 'NULL') as json_value,
                            COALESCE(CAST(bq_table.${field} AS STRING), 'NULL') as bq_value,
                            CASE 
                                WHEN COALESCE(CAST(json_table.${field} AS STRING), 'NULL') = 
                                     COALESCE(CAST(bq_table.${field} AS STRING), 'NULL') 
                                THEN 'MATCH' 
                                ELSE 'DIFFER' 
                            END as comparison_result
                        FROM \`${tempTableId}\` json_table
                        INNER JOIN \`${sourceTableName}\` bq_table
                        ON json_table.${primaryKey} = bq_table.${primaryKey}
                        WHERE json_table.${primaryKey} IN (${matchedIdsStr})
                        LIMIT 100
                    `;
                    
                    const [fieldResult] = await this.bigquery.query(fieldComparisonQuery);
                    
                    const differences = fieldResult.filter(r => r.comparison_result === 'DIFFER');
                    const matches = fieldResult.filter(r => r.comparison_result === 'MATCH');
                    
                    console.log(`Field ${field}: ${matches.length} matches, ${differences.length} differences`);
                    
                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: fieldResult.length,
                        perfectMatches: matches.length,
                        differences: differences.length,
                        matchRate: fieldResult.length > 0 ? ((matches.length / fieldResult.length) * 100).toFixed(1) : '0.0',
                        sampleDifferences: differences.slice(0, 3),
                        allComparisons: fieldResult.slice(0, 10)
                    });
                    
                    totalFieldIssues += differences.length;
                    
                } catch (fieldError) {
                    console.warn(`Skipping field ${field}:`, fieldError.message);
                    
                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: 0,
                        perfectMatches: 0,
                        differences: 0,
                        matchRate: '0.0',
                        error: fieldError.message,
                        sampleDifferences: [],
                        allComparisons: []
                    });
                }
            }
            
            console.log(`Field analysis completed: ${totalFieldIssues} total field issues found across ${safeFields.length} fields`);
            
            return {
                totalFieldIssues: totalFieldIssues,
                fieldComparison: fieldComparison,
                fieldsAnalyzed: safeFields.length,
                recordsAnalyzed: matchedIds.length,
                perfectFields: fieldComparison.filter(f => f.differences === 0 && !f.error).length,
                problematicFields: fieldComparison.filter(f => f.differences > 0 || f.error).length,
                summary: `Analyzed ${safeFields.length} common fields across ${matchedIds.length} matched records`
            };
            
        } catch (error) {
            console.error('Common field analysis failed:', error.message);
            return {
                totalFieldIssues: 0,
                fieldComparison: [],
                fieldsAnalyzed: 0,
                recordsAnalyzed: 0,
                perfectFields: 0,
                problematicFields: 0,
                summary: 'Field analysis failed: ' + error.message
            };
        }
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

            let allDuplicateRecords = [];
            if (hasDuplicates) {
                const sampleFieldsQuery = `SELECT * FROM \`${tempTableId}\` LIMIT 1`;
                const [sampleResult] = await this.bigquery.query(sampleFieldsQuery);
                const availableFields = sampleResult.length > 0 ? Object.keys(sampleResult[0]) : [primaryKey];
                
                const displayFields = [
                    primaryKey,
                    ...availableFields.filter(field => 
                        field !== primaryKey &&
                        (field.toLowerCase().includes('name') || 
                         field.toLowerCase().includes('description') || 
                         field.toLowerCase().includes('type') ||
                         field.toLowerCase().includes('account') || 
                         field.toLowerCase().includes('status') || 
                         field.toLowerCase().includes('created') ||
                         field.toLowerCase().includes('code') || 
                         field.toLowerCase().includes('arn') || 
                         field.toLowerCase().includes('catalog'))
                    ).slice(0, 4)
                ];

                const duplicateRecordsQuery = `
                    SELECT 
                        ${displayFields.map(field => `${field}`).join(', ')}
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IN (
                        SELECT ${primaryKey}
                        FROM \`${tempTableId}\`
                        WHERE ${primaryKey} IS NOT NULL
                        GROUP BY ${primaryKey}
                        HAVING COUNT(*) > 1
                    )
                    ORDER BY ${primaryKey}
                    LIMIT 100
                `;

                try {
                    const [duplicateRecords] = await this.bigquery.query(duplicateRecordsQuery);
                    allDuplicateRecords = duplicateRecords;
                } catch (recordsError) {
                    console.warn(`Could not get duplicate record details:`, recordsError.message);
                }
            }

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
                allDuplicateRecords: allDuplicateRecords,
                recommendations: recommendations
            };

        } catch (error) {
            console.error('Duplicates analysis failed:', error.message);
            return {
                hasDuplicates: false,
                duplicateCount: 0,
                totalDuplicateRecords: 0,
                duplicateKeys: [],
                allDuplicateRecords: [],
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
                message: 'Using dynamic schema-safe common field analysis'
            };
        } catch (error) {
            console.error('Legacy compareFieldDifferences failed:', error.message);
            throw error;
        }
    }
}

module.exports = ComparisonEngineService;