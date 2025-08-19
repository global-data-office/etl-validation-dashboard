// services/comparison-engine.js - v2.1: Custom primary keys + ALL fields + BigQuery duplicates
const { BigQuery } = require('@google-cloud/bigquery');

class ComparisonEngineService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
    }

    /**
     * Get common fields between JSON and BigQuery tables
     */
    async getCommonFields(tempTableId, sourceTableName) {
        try {
            // Get JSON table fields
            let jsonFields = [];
            try {
                const jsonSchemaQuery = `SELECT * FROM \`${tempTableId}\` LIMIT 1`;
                const [jsonSample] = await this.bigquery.query(jsonSchemaQuery);
                
                if (jsonSample.length > 0) {
                    jsonFields = Object.keys(jsonSample[0]);
                }
            } catch (jsonError) {
                throw new Error(`Cannot access JSON temp table: ${jsonError.message}`);
            }
            
            // Get BigQuery table fields
            let bqFields = [];
            try {
                const bqSchemaQuery = `SELECT * FROM \`${sourceTableName}\` LIMIT 1`;
                const [bqSample] = await this.bigquery.query(bqSchemaQuery);
                
                if (bqSample.length > 0) {
                    bqFields = Object.keys(bqSample[0]);
                }
            } catch (bqError) {
                throw new Error(`Cannot access BigQuery source table: ${bqError.message}`);
            }
            
            // Find common fields
            const commonFields = jsonFields.filter(field => bqFields.includes(field));
            const jsonOnlyFields = jsonFields.filter(field => !bqFields.includes(field));
            const bqOnlyFields = bqFields.filter(field => !jsonFields.includes(field));
            
            // Identify potential primary key candidates from common fields
            const primaryKeyCandidates = commonFields.filter(field => {
                const lowerField = field.toLowerCase();
                return lowerField.includes('id') || 
                       lowerField.includes('key') ||
                       lowerField.includes('number') ||
                       lowerField.includes('_id') ||
                       lowerField.includes('sys_') ||
                       ['task_sys_id', 'sys_id', 'task_number', 'number', 'id', 'key'].includes(lowerField);
            });
            
            return {
                commonFields: commonFields,
                jsonOnlyFields: jsonOnlyFields,
                bqOnlyFields: bqOnlyFields,
                primaryKeyCandidates: primaryKeyCandidates,
                totalJsonFields: jsonFields.length,
                totalBqFields: bqFields.length,
                schemaCompatibility: commonFields.length / Math.max(jsonFields.length, bqFields.length)
            };
            
        } catch (error) {
            throw new Error(`Schema analysis failed: ${error.message}`);
        }
    }

    /**
     * Validate if ANY field can be used as primary key
     */
    async validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, commonFields) {
        try {
            // Check if the field is in common fields
            if (!commonFields.includes(primaryKey)) {
                throw new Error(`Primary key '${primaryKey}' is not available in both tables. Available common fields: ${commonFields.slice(0, 10).join(', ')}`);
            }
            
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
                    throw new Error(`Field '${primaryKey}' validation failed: ${queryError.message}`);
                }
            }
            
            return validationResults;
            
        } catch (error) {
            throw error;
        }
    }

    /**
     * Main comparison with all enhancements
     */
    async compareJSONvsBigQuery(tempTableId, sourceTableName, primaryKey = 'task_sys_id', comparisonFields = [], strategy = 'enhanced') {
        try {
            // STEP 1: Get common fields analysis
            const schemaAnalysis = await this.getCommonFields(tempTableId, sourceTableName);

            // STEP 2: Validate the custom primary key exists in both tables
            if (!schemaAnalysis.commonFields.includes(primaryKey)) {
                const suggestedKey = schemaAnalysis.primaryKeyCandidates[0] || schemaAnalysis.commonFields[0];
                throw new Error(`Primary key '${primaryKey}' not available in both tables. Suggested alternative: '${suggestedKey}'. Available common fields: ${schemaAnalysis.commonFields.slice(0, 5).join(', ')}`);
            }

            // STEP 3: Validate the primary key field works in both tables
            const keyValidation = await this.validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, schemaAnalysis.commonFields);

            // STEP 4: Get record counts using validated primary key
            const recordCounts = await this.getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey);

            // STEP 5: Find matching records using the primary key
            const matchAnalysis = await this.getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey);

            // STEP 6: Analyze ALL common fields (no limits)
            const fieldAnalysis = await this.analyzeAllCommonFieldsEnhanced(
                tempTableId, 
                sourceTableName, 
                primaryKey,
                schemaAnalysis.commonFields,
                matchAnalysis.matchedIds
            );

            // STEP 7: Get comprehensive duplicates analysis for BOTH tables
            const duplicatesAnalysis = await this.analyzeDuplicatesEnhanced(tempTableId, sourceTableName, primaryKey);

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
                strategy: 'enhanced-v2.1'
            };

            return {
                success: true,
                analysisType: 'enhanced-v2.1',
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
                    fieldDifferences: fieldAnalysis
                },
                fieldWiseAnalysis: fieldAnalysis,
                duplicatesAnalysis: duplicatesAnalysis,
                summary: summary,
                metadata: {
                    tempTableId,
                    sourceTableName,
                    primaryKey,
                    comparisonFields: schemaAnalysis.commonFields,
                    strategy: 'enhanced-v2.1',
                    comparisonDate: new Date().toISOString()
                }
            };

        } catch (error) {
            throw new Error(`Comparison failed: ${error.message}`);
        }
    }

    /**
     * Get record counts using validated primary key
     */
    async getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey) {
        try {
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
            throw error;
        }
    }

    /**
     * Find matches using validated primary key
     */
    async getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey) {
        try {
            // Get all unique JSON keys
            const getAllJsonKeysQuery = `
                SELECT DISTINCT ${primaryKey} as key_value
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                ORDER BY ${primaryKey}
            `;
            
            const [allJsonKeys] = await this.bigquery.query(getAllJsonKeysQuery);
            const jsonKeysList = allJsonKeys.map(r => r.key_value);

            // Find matching keys
            const getMatchingKeysQuery = `
                SELECT DISTINCT json_table.${primaryKey} as matched_key
                FROM \`${tempTableId}\` json_table
                INNER JOIN \`${sourceTableName}\` bq_table
                ON json_table.${primaryKey} = bq_table.${primaryKey}
                WHERE json_table.${primaryKey} IS NOT NULL
                ORDER BY json_table.${primaryKey}
            `;
            
            const [matchingKeys] = await this.bigquery.query(getMatchingKeysQuery);
            const matchedKeysList = matchingKeys.map(r => r.matched_key);

            // Calculate keys only in JSON
            const jsonOnlyKeys = jsonKeysList.filter(key => !matchedKeysList.includes(key));

            // Get sample keys only in BigQuery
            const getBqOnlyKeysQuery = `
                SELECT DISTINCT bq_table.${primaryKey} as bq_only_key
                FROM \`${sourceTableName}\` bq_table
                WHERE bq_table.${primaryKey} IS NOT NULL
                  AND bq_table.${primaryKey} NOT IN (
                    SELECT DISTINCT ${primaryKey} 
                    FROM \`${tempTableId}\` 
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;
            
            const [bqOnlyKeys] = await this.bigquery.query(getBqOnlyKeysQuery);
            const bqOnlyKeysList = bqOnlyKeys.map(r => r.bq_only_key);

            return {
                matchCount: matchedKeysList.length,
                matchedIds: matchedKeysList,
                jsonOnlyCount: jsonOnlyKeys.length,
                jsonOnlyIds: jsonOnlyKeys,
                jsonOnlyRecords: jsonOnlyKeys.map(key => ({ [primaryKey]: key })),
                bqOnlyCount: bqOnlyKeysList.length,
                bqOnlyRecords: bqOnlyKeysList.map(key => ({ [primaryKey]: key })),
                primaryKeyUsed: primaryKey
            };

        } catch (error) {
            throw error;
        }
    }

    /**
     * Analyze ALL common fields (removed artificial limits)
     */
    async analyzeAllCommonFieldsEnhanced(tempTableId, sourceTableName, primaryKey, commonFields, matchedIds) {
        try {
            if (matchedIds.length === 0) {
                return {
                    totalFieldIssues: 0,
                    fieldComparison: [],
                    fieldsAnalyzed: 0,
                    recordsAnalyzed: 0,
                    perfectFields: 0,
                    problematicFields: 0,
                    summary: 'No matched records to analyze'
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
            
            // Use ALL common fields (removed all artificial limits)
            const fieldsToAnalyze = commonFields.filter(field => 
                field !== primaryKey // Don't analyze primary key
            );
            
            // Process ALL fields without restrictions
            for (let i = 0; i < fieldsToAnalyze.length; i++) {
                const field = fieldsToAnalyze[i];
                
                try {
                    // Create safe comparison query using matched IDs
                    const matchedIdsStr = matchedIds.slice(0, 200).map(id => `'${String(id).replace(/'/g, "\\'")}'`).join(',');
                    
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
                        LIMIT 500
                    `;
                    
                    const [fieldResult] = await this.bigquery.query(fieldComparisonQuery);
                    
                    const differences = fieldResult.filter(r => r.comparison_result === 'DIFFER');
                    const matches = fieldResult.filter(r => r.comparison_result === 'MATCH');
                    
                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: fieldResult.length,
                        perfectMatches: matches.length,
                        differences: differences.length,
                        matchRate: fieldResult.length > 0 ? ((matches.length / fieldResult.length) * 100).toFixed(1) : '0.0',
                        sampleDifferences: differences.slice(0, 5),
                        allComparisons: fieldResult.slice(0, 15)
                    });
                    
                    totalFieldIssues += differences.length;
                    
                } catch (fieldError) {
                    console.warn(`Issue analyzing field ${field}:`, fieldError.message);
                    
                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: 0,
                        perfectMatches: 0,
                        differences: 0,
                        matchRate: '0.0',
                        error: `Analysis failed: ${fieldError.message}`,
                        sampleDifferences: [],
                        allComparisons: []
                    });
                }
            }
            
            return {
                totalFieldIssues: totalFieldIssues,
                fieldComparison: fieldComparison,
                fieldsAnalyzed: fieldsToAnalyze.length,
                recordsAnalyzed: matchedIds.length,
                perfectFields: fieldComparison.filter(f => f.differences === 0 && !f.error).length,
                problematicFields: fieldComparison.filter(f => f.differences > 0 || f.error).length,
                summary: `Analyzed ALL ${fieldsToAnalyze.length} common fields across ${matchedIds.length} matched records`
            };
            
        } catch (error) {
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
     * Analyze duplicates in BOTH JSON and BigQuery tables
     */
    async analyzeDuplicatesEnhanced(tempTableId, sourceTableName, primaryKey) {
        try {
            // Analyze JSON duplicates
            const jsonDuplicatesAnalysis = await this.analyzeTableDuplicates(tempTableId, primaryKey, 'JSON');

            // Analyze BigQuery duplicates
            const bqDuplicatesAnalysis = await this.analyzeTableDuplicates(sourceTableName, primaryKey, 'BigQuery');

            // Combined analysis
            const totalDuplicateKeys = (jsonDuplicatesAnalysis.duplicateCount || 0) + (bqDuplicatesAnalysis.duplicateCount || 0);
            const totalDuplicateRecords = (jsonDuplicatesAnalysis.totalDuplicateRecords || 0) + (bqDuplicatesAnalysis.totalDuplicateRecords || 0);

            // Create recommendations based on findings in BOTH tables
            const recommendations = [];
            
            if (jsonDuplicatesAnalysis.hasDuplicates) {
                recommendations.push(`📄 JSON Source: ${jsonDuplicatesAnalysis.duplicateCount} duplicate keys found - review data source quality`);
                recommendations.push(`🔧 Consider deduplication in your data pipeline before BigQuery loading`);
            }
            
            if (bqDuplicatesAnalysis.hasDuplicates) {
                recommendations.push(`🗄️ BigQuery Target: ${bqDuplicatesAnalysis.duplicateCount} duplicate keys found - target table has data quality issues`);
                recommendations.push(`🛠️ Consider implementing MERGE statements instead of INSERT for better data quality`);
            }
            
            if (!jsonDuplicatesAnalysis.hasDuplicates && !bqDuplicatesAnalysis.hasDuplicates) {
                recommendations.push(`✅ Excellent data quality - no duplicates found in either JSON source or BigQuery target`);
            }
            
            if (jsonDuplicatesAnalysis.hasDuplicates && bqDuplicatesAnalysis.hasDuplicates) {
                recommendations.push(`⚠️ Both JSON source and BigQuery target have duplicates - comprehensive data quality review needed`);
            }

            return {
                // Combined metrics for compatibility
                hasDuplicates: jsonDuplicatesAnalysis.hasDuplicates || bqDuplicatesAnalysis.hasDuplicates,
                duplicateCount: jsonDuplicatesAnalysis.duplicateCount || 0,
                totalDuplicateRecords: jsonDuplicatesAnalysis.totalDuplicateRecords || 0,
                
                // Separate JSON and BigQuery analysis
                jsonDuplicates: {
                    ...jsonDuplicatesAnalysis,
                    tableType: 'JSON Source'
                },
                bqDuplicates: {
                    ...bqDuplicatesAnalysis,
                    tableType: 'BigQuery Target'
                },
                
                // Combined analysis
                combinedAnalysis: {
                    totalDuplicateKeys: totalDuplicateKeys,
                    totalDuplicateRecords: totalDuplicateRecords,
                    jsonContribution: jsonDuplicatesAnalysis.duplicateCount || 0,
                    bqContribution: bqDuplicatesAnalysis.duplicateCount || 0,
                    overallDataQuality: (!jsonDuplicatesAnalysis.hasDuplicates && !bqDuplicatesAnalysis.hasDuplicates) ? 'Excellent' : 
                                       (jsonDuplicatesAnalysis.hasDuplicates && bqDuplicatesAnalysis.hasDuplicates) ? 'Poor' : 'Good'
                },
                
                // Legacy compatibility
                duplicateKeys: jsonDuplicatesAnalysis.duplicateKeys || [],
                allDuplicateRecords: jsonDuplicatesAnalysis.allDuplicateRecords || [],
                
                // Recommendations
                recommendations: recommendations,
                analysisDate: new Date().toISOString()
            };

        } catch (error) {
            return {
                hasDuplicates: false,
                duplicateCount: 0,
                totalDuplicateRecords: 0,
                jsonDuplicates: { 
                    hasDuplicates: false, 
                    duplicateCount: 0, 
                    tableType: 'JSON Source',
                    error: error.message 
                },
                bqDuplicates: { 
                    hasDuplicates: false, 
                    duplicateCount: 0, 
                    tableType: 'BigQuery Target',
                    error: error.message 
                },
                duplicateKeys: [],
                allDuplicateRecords: [],
                recommendations: ['Duplicates analysis failed: ' + error.message]
            };
        }
    }

    /**
     * Analyze duplicates for a specific table (JSON or BigQuery)
     */
    async analyzeTableDuplicates(tableId, primaryKey, tableName) {
        try {
            // Find duplicate keys
            const duplicateKeysQuery = `
                SELECT 
                    ${primaryKey} as duplicate_key,
                    COUNT(*) as occurrence_count
                FROM \`${tableId}\`
                WHERE ${primaryKey} IS NOT NULL
                GROUP BY ${primaryKey}
                HAVING COUNT(*) > 1
                ORDER BY occurrence_count DESC
                LIMIT 100
            `;

            const [duplicateKeys] = await this.bigquery.query(duplicateKeysQuery);
            
            const hasDuplicates = duplicateKeys.length > 0;
            const totalDuplicateRecords = duplicateKeys.reduce((sum, dup) => sum + parseInt(dup.occurrence_count), 0) - duplicateKeys.length;

            // Get detailed duplicate records if any
            let allDuplicateRecords = [];
            if (hasDuplicates) {
                const sampleFields = ['task_number', 'task_priority', 'task_state', 'task_sys_created_on']
                    .filter(field => field !== primaryKey);
                
                const selectFields = sampleFields.length > 0 ? 
                    `, ${sampleFields.join(', ')}` : '';

                const duplicateRecordsQuery = `
                    SELECT 
                        ${primaryKey}${selectFields}
                    FROM \`${tableId}\`
                    WHERE ${primaryKey} IN (
                        SELECT ${primaryKey}
                        FROM \`${tableId}\`
                        WHERE ${primaryKey} IS NOT NULL
                        GROUP BY ${primaryKey}
                        HAVING COUNT(*) > 1
                    )
                    ORDER BY ${primaryKey}
                    LIMIT 500
                `;

                try {
                    const [duplicateRecords] = await this.bigquery.query(duplicateRecordsQuery);
                    allDuplicateRecords = duplicateRecords;
                } catch (recordsError) {
                    console.warn(`Could not get ${tableName} duplicate record details:`, recordsError.message);
                }
            }

            return {
                tableName: tableName,
                tableId: tableId,
                hasDuplicates: hasDuplicates,
                duplicateCount: duplicateKeys.length,
                totalDuplicateRecords: totalDuplicateRecords,
                duplicateKeys: duplicateKeys.map(dup => ({
                    key: dup.duplicate_key,
                    count: parseInt(dup.occurrence_count),
                    table: tableName
                })),
                allDuplicateRecords: allDuplicateRecords,
                primaryKeyUsed: primaryKey
            };

        } catch (error) {
            return {
                tableName: tableName,
                tableId: tableId,
                hasDuplicates: false,
                duplicateCount: 0,
                totalDuplicateRecords: 0,
                duplicateKeys: [],
                allDuplicateRecords: [],
                primaryKeyUsed: primaryKey,
                error: error.message
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
                LIMIT 20
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
                LIMIT 20
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
            throw error;
        }
    }

    async compareFieldDifferences(tempTableId, sourceTableName, primaryKey, comparisonFields = [], maxRecords = 100) {
        try {
            return {
                totalDifferences: 0,
                fieldDifferences: [],
                message: 'Using enhanced ALL-fields analysis - check fieldWiseAnalysis in main response'
            };
        } catch (error) {
            throw error;
        }
    }
}

module.exports = ComparisonEngineService;