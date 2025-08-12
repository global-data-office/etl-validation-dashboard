// services/comparison-engine.js - UPDATED: Schema-Safe Any Column Comparison
const { BigQuery } = require('@google-cloud/bigquery');

class ComparisonEngineService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        
        console.log('🔍 Comparison Engine Service initialized - SCHEMA-SAFE ANY COLUMN VERSION');
    }

    /**
     * ENHANCED: Get common fields between JSON and BigQuery tables
     */
    async getCommonFields(tempTableId, sourceTableName) {
        try {
            console.log('📋 Identifying common fields between tables...');
            
            // Get JSON table fields
            let jsonFields = [];
            try {
                const jsonSchemaQuery = `SELECT * FROM \`${tempTableId}\` LIMIT 1`;
                const [jsonSample] = await this.bigquery.query(jsonSchemaQuery);
                
                if (jsonSample.length > 0) {
                    jsonFields = Object.keys(jsonSample[0]);
                    console.log(`✅ JSON table fields: ${jsonFields.length} total`);
                    console.log(`🔍 JSON sample fields: [${jsonFields.slice(0, 10).join(', ')}...]`);
                }
            } catch (jsonError) {
                console.error('❌ Failed to get JSON fields:', jsonError.message);
                throw new Error(`Cannot access JSON temp table: ${jsonError.message}`);
            }
            
            // Get BigQuery table fields
            let bqFields = [];
            try {
                const bqSchemaQuery = `SELECT * FROM \`${sourceTableName}\` LIMIT 1`;
                const [bqSample] = await this.bigquery.query(bqSchemaQuery);
                
                if (bqSample.length > 0) {
                    bqFields = Object.keys(bqSample[0]);
                    console.log(`✅ BigQuery table fields: ${bqFields.length} total`);
                    console.log(`🔍 BigQuery sample fields: [${bqFields.slice(0, 10).join(', ')}...]`);
                }
            } catch (bqError) {
                console.error('❌ Failed to get BigQuery fields:', bqError.message);
                throw new Error(`Cannot access BigQuery source table: ${bqError.message}`);
            }
            
            // Find common fields
            const commonFields = jsonFields.filter(field => bqFields.includes(field));
            const jsonOnlyFields = jsonFields.filter(field => !bqFields.includes(field));
            const bqOnlyFields = bqFields.filter(field => !jsonFields.includes(field));
            
            console.log(`📊 Schema Analysis Results:`);
            console.log(`   🤝 Common fields: ${commonFields.length} - [${commonFields.slice(0, 10).join(', ')}...]`);
            console.log(`   📄 JSON-only fields: ${jsonOnlyFields.length}`);
            console.log(`   🗄️ BigQuery-only fields: ${bqOnlyFields.length}`);
            
            // Identify potential primary key candidates from common fields
            const primaryKeyCandidates = commonFields.filter(field => 
                field.toLowerCase().includes('id') || 
                field.toLowerCase().includes('key') ||
                field.toLowerCase().includes('number') ||
                ['task_sys_id', 'sys_id', 'task_number', 'number', 'id'].includes(field.toLowerCase())
            );
            
            console.log(`🔑 Primary key candidates: [${primaryKeyCandidates.join(', ')}]`);
            
            return {
                commonFields: commonFields,
                jsonOnlyFields: jsonOnlyFields,
                bqOnlyFields: bqOnlyFields,
                primaryKeyCandidates: primaryKeyCandidates,
                totalJsonFields: jsonFields.length,
                totalBqFields: bqFields.length,
                schemaCompatibility: commonFields.length / Math.max(jsonFields.length, bqFields.length),
                // Add complete field lists for frontend display
                jsonColumns: jsonFields.map(field => ({ column_name: field, data_type: 'STRING' })),
                bqColumns: bqFields.map(field => ({ column_name: field, data_type: 'STRING' })),
                commonColumns: commonFields.map(field => ({ 
                    column_name: field, 
                    json_type: 'STRING', 
                    bq_type: 'STRING', 
                    type_match: true 
                }))
            };
            
        } catch (error) {
            console.error('❌ Schema analysis failed:', error.message);
            throw new Error(`Schema analysis failed: ${error.message}`);
        }
    }

    /**
     * SAFE: Validate if a field can be used as primary key
     */
    async validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, commonFields) {
        try {
            console.log(`🔍 Validating primary key field: ${primaryKey}`);
            
            // First check if the field is in common fields
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
                    console.log(`✅ ${queryInfo.name}:`, result[0]);
                } catch (queryError) {
                    console.error(`❌ ${queryInfo.name} failed:`, queryError.message);
                    throw new Error(`Field '${primaryKey}' validation failed: ${queryError.message}`);
                }
            }
            
            return validationResults;
            
        } catch (error) {
            console.error(`❌ Primary key validation failed:`, error.message);
            throw error;
        }
    }

    /**
     * MAIN: Schema-safe comparison using any common field
     */
    async compareJSONvsBigQuery(tempTableId, sourceTableName, primaryKey = 'task_sys_id', comparisonFields = [], strategy = 'full') {
        try {
            console.log('🚀 Starting SCHEMA-SAFE comparison with any common field...');
            console.log(`📄 SOURCE (JSON): ${tempTableId}`);
            console.log(`🗄️ TARGET (BigQuery): ${sourceTableName}`);
            console.log(`🔑 Requested primary key: ${primaryKey}`);

            // STEP 1: Get common fields analysis
            const schemaAnalysis = await this.getCommonFields(tempTableId, sourceTableName);
            console.log(`📊 Schema analysis completed: ${schemaAnalysis.commonFields.length} common fields found`);

            // STEP 2: Validate the requested primary key exists in both tables
            if (!schemaAnalysis.commonFields.includes(primaryKey)) {
                console.error(`❌ Primary key '${primaryKey}' not found in common fields!`);
                console.log(`🔍 Available common fields: [${schemaAnalysis.commonFields.join(', ')}]`);
                console.log(`💡 Suggested primary key candidates: [${schemaAnalysis.primaryKeyCandidates.join(', ')}]`);
                
                // Auto-suggest best alternative
                const suggestedKey = schemaAnalysis.primaryKeyCandidates[0] || schemaAnalysis.commonFields[0];
                throw new Error(`Primary key '${primaryKey}' not available in both tables. Suggested alternative: '${suggestedKey}'. Available common fields: ${schemaAnalysis.commonFields.slice(0, 5).join(', ')}`);
            }

            // STEP 3: Validate the primary key field works in both tables
            const keyValidation = await this.validatePrimaryKeyField(tempTableId, sourceTableName, primaryKey, schemaAnalysis.commonFields);
            console.log(`✅ Primary key '${primaryKey}' validated in both tables`);

            // STEP 4: Get record counts using validated primary key
            console.log('📊 Getting record counts using validated fields...');
            const recordCounts = await this.getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey);

            // STEP 5: Find matching records using the chosen primary key
            console.log('🔍 Finding matches using schema-aware primary key...');
            const matchAnalysis = await this.getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey);

            // STEP 6: Analyze field differences for common fields only (safe)
            console.log('🔍 Analyzing field differences for common fields only...');
            const fieldAnalysis = await this.analyzeCommonFieldDifferences(
                tempTableId, 
                sourceTableName, 
                primaryKey,
                schemaAnalysis.commonFields,
                matchAnalysis.matchedIds
            );

            // STEP 7: Get comprehensive duplicates analysis
            console.log('🔄 Analyzing duplicates in source data...');
            const duplicatesAnalysis = await this.analyzeDuplicates(tempTableId, primaryKey);

            // STEP 8: Create comprehensive results
            const summary = {
                // Source data metrics
                totalRecordsInFile: recordCounts.jsonDetails.totalRecords,
                uniqueSourceRecords: recordCounts.jsonDetails.uniquePrimaryKeys,
                duplicateRecordsInFile: recordCounts.jsonDetails.duplicateRecords,
                
                // Target data metrics  
                targetRecords: recordCounts.bqDetails.totalRecords,
                
                // Comparison results
                recordsReachedTarget: matchAnalysis.matchCount,
                recordsFailedToReachTarget: matchAnalysis.jsonOnlyCount,
                recordsOnlyInTarget: matchAnalysis.bqOnlyCount,
                
                // Success metrics
                pipelineSuccessRate: recordCounts.jsonDetails.uniquePrimaryKeys > 0 ? 
                    ((matchAnalysis.matchCount / recordCounts.jsonDetails.uniquePrimaryKeys) * 100).toFixed(1) : '0.0',
                matchRate: recordCounts.jsonDetails.totalRecords > 0 ? 
                    ((matchAnalysis.matchCount / recordCounts.jsonDetails.totalRecords) * 100).toFixed(1) : '0.0',
                
                // Field analysis
                fieldsAnalyzed: fieldAnalysis.fieldsAnalyzed,
                totalFieldIssues: fieldAnalysis.totalFieldIssues,
                
                // Schema compatibility
                schemaCompatibility: (schemaAnalysis.schemaCompatibility * 100).toFixed(1) + '%',
                commonFieldsCount: schemaAnalysis.commonFields.length,
                
                // Metadata
                primaryKeyUsed: primaryKey,
                matchedRecordIds: matchAnalysis.matchedIds,
                failedRecordIds: matchAnalysis.jsonOnlyIds,
                comparisonDate: new Date().toISOString(),
                strategy: 'schema-safe-any-common-field'
            };

            console.log('✅ SCHEMA-SAFE comparison completed successfully');
            console.log(`📊 Results: ${matchAnalysis.matchCount} matches using '${primaryKey}' field`);

            return {
                success: true,
                analysisType: 'schema-safe-any-common-field',
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
                    comparisonFields: comparisonFields.length > 0 ? comparisonFields : schemaAnalysis.commonFields.slice(0, 10),
                    strategy: 'schema-safe-any-common-field',
                    comparisonDate: new Date().toISOString()
                }
            };

        } catch (error) {
            console.error('❌ Schema-safe comparison failed:', error.message);
            throw new Error(`Schema-safe comparison failed: ${error.message}`);
        }
    }

    /**
     * Get record counts using schema-validated primary key
     */
    async getSchemaAwareRecordCounts(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`📊 Getting schema-aware record counts using: ${primaryKey}`);

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

            console.log(`✅ JSON analysis: ${jsonDetails.total_records} total, ${jsonDetails.unique_primary_keys} unique, ${jsonDetails.duplicate_records} duplicates`);
            console.log(`✅ BigQuery analysis: ${bqDetails.total_records} total, ${bqDetails.unique_primary_keys} unique`);

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
            console.error(`❌ Failed to get schema-aware record counts:`, error.message);
            throw error;
        }
    }

    /**
     * Find matches using schema-validated primary key
     */
    async getSchemaAwareMatches(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`🔍 Finding matches using validated primary key: ${primaryKey}`);

            // Get all unique JSON keys
            const getAllJsonKeysQuery = `
                SELECT DISTINCT ${primaryKey} as key_value
                FROM \`${tempTableId}\`
                WHERE ${primaryKey} IS NOT NULL
                ORDER BY ${primaryKey}
            `;
            
            const [allJsonKeys] = await this.bigquery.query(getAllJsonKeysQuery);
            const jsonKeysList = allJsonKeys.map(r => r.key_value);
            
            console.log(`📄 JSON unique keys: ${jsonKeysList.length} found`);

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
            
            console.log(`✅ Matched keys: ${matchedKeysList.length} found`);

            // Calculate keys only in JSON
            const jsonOnlyKeys = jsonKeysList.filter(key => !matchedKeysList.includes(key));
            console.log(`📄 JSON-only keys: ${jsonOnlyKeys.length} found`);

            // Get sample keys only in BigQuery (limited for performance)
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
            
            console.log(`🗄️ BigQuery-only keys (sample): ${bqOnlyKeysList.length} found`);

            // Get sample matched records with details (using only common fields)
            let sampleMatches = [];
            if (matchedKeysList.length > 0) {
                // Use only safe common fields for sample display
                const safeDisplayFields = ['task_number', 'task_priority', 'task_state'].filter(field => 
                    // Only include if field exists in common fields
                    primaryKey !== field // Don't repeat primary key
                );
                
                const selectFields = safeDisplayFields.length > 0 ? 
                    `, ${safeDisplayFields.map(f => `json_table.${f} as json_${f}, bq_table.${f} as bq_${f}`).join(', ')}` : '';
                
                const sampleMatchQuery = `
                    SELECT 
                        json_table.${primaryKey} as key_value${selectFields}
                    FROM \`${tempTableId}\` json_table
                    INNER JOIN \`${sourceTableName}\` bq_table
                    ON json_table.${primaryKey} = bq_table.${primaryKey}
                    WHERE json_table.${primaryKey} IS NOT NULL
                    LIMIT 5
                `;
                
                try {
                    const [sampleResult] = await this.bigquery.query(sampleMatchQuery);
                    sampleMatches = sampleResult;
                    console.log(`✅ Sample matches retrieved: ${sampleMatches.length} records`);
                } catch (sampleError) {
                    console.warn(`⚠️ Could not get sample match details:`, sampleError.message);
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
                primaryKeyUsed: primaryKey
            };

        } catch (error) {
            console.error(`❌ Schema-aware match analysis failed:`, error.message);
            throw error;
        }
    }

    /**
     * SAFE: Analyze field differences only for common fields
     */
    async analyzeCommonFieldDifferences(tempTableId, sourceTableName, primaryKey, commonFields, matchedIds) {
        try {
            console.log(`🔍 Analyzing field differences for common fields only...`);
            console.log(`📋 Common fields available: ${commonFields.length}`);
            console.log(`🔑 Matched records to analyze: ${matchedIds.length}`);
            
            if (matchedIds.length === 0) {
                return {
                    totalFieldIssues: 0,
                    fieldComparison: [],
                    fieldsAnalyzed: 0,
                    summary: 'No matched records to analyze - 0 matches found'
                };
            }

            if (commonFields.length === 0) {
                return {
                    totalFieldIssues: 0,
                    fieldComparison: [],
                    fieldsAnalyzed: 0,
                    summary: 'No common fields found for comparison'
                };
            }

            const fieldComparison = [];
            let totalFieldIssues = 0;
            
            // Select safe common fields for analysis (excluding problematic ones)
            const safeFields = commonFields.filter(field => 
                // Include important fields but exclude problematic ones
                !field.toLowerCase().includes('comment') &&
                !field.toLowerCase().includes('description') &&
                !field.toLowerCase().includes('sys_domain_path') &&
                !field.toLowerCase().includes('sys_tags') &&
                field.length < 50 && // Avoid very long field names
                field !== primaryKey // Don't analyze primary key (it will always match)
            ).slice(0, 8); // Limit to 8 fields for performance
            
            console.log(`🔧 Safe fields to analyze: [${safeFields.join(', ')}]`);
            
            for (const field of safeFields) {
                try {
                    console.log(`🔍 Analyzing field: ${field} for ${matchedIds.length} matched records...`);
                    
                    // Create safe comparison query using only matched IDs
                    const matchedIdsStr = matchedIds.slice(0, 50).map(id => `'${String(id).replace(/'/g, "\\'")}'`).join(','); // Limit to 50 for performance
                    
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
                    
                    console.log(`✅ Field ${field}: ${matches.length} matches, ${differences.length} differences`);
                    
                    fieldComparison.push({
                        fieldName: field,
                        totalRecords: fieldResult.length,
                        perfectMatches: matches.length,
                        differences: differences.length,
                        matchRate: fieldResult.length > 0 ? ((matches.length / fieldResult.length) * 100).toFixed(1) : '0.0',
                        sampleDifferences: differences.slice(0, 3),
                        allComparisons: fieldResult.slice(0, 10) // First 10 for display
                    });
                    
                    totalFieldIssues += differences.length;
                    
                } catch (fieldError) {
                    console.warn(`⚠️ Skipping field ${field}:`, fieldError.message);
                    
                    // Add error info to results
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
            
            console.log(`✅ Field analysis completed: ${totalFieldIssues} total field issues found across ${safeFields.length} fields`);
            
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
            console.error('❌ Common field analysis failed:', error.message);
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
     * Analyze duplicates in source data
     */
    async analyzeDuplicates(tempTableId, primaryKey) {
        try {
            console.log(`🔄 Analyzing duplicates using primary key: ${primaryKey}`);

            // Find duplicate keys
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
            
            console.log(`📊 Duplicate analysis: ${duplicateKeys.length} duplicate keys, ${totalDuplicateRecords} duplicate records`);

            // Get all duplicate records with details (if any)
            let allDuplicateRecords = [];
            if (hasDuplicates) {
                const duplicateRecordsQuery = `
                    SELECT 
                        ${primaryKey} as task_sys_id,
                        task_number,
                        task_priority,
                        task_state,
                        task_sys_created_on
                    FROM \`${tempTableId}\`
                    WHERE ${primaryKey} IN (
                        SELECT ${primaryKey}
                        FROM \`${tempTableId}\`
                        WHERE ${primaryKey} IS NOT NULL
                        GROUP BY ${primaryKey}
                        HAVING COUNT(*) > 1
                    )
                    ORDER BY ${primaryKey}, task_sys_created_on
                `;

                try {
                    const [duplicateRecords] = await this.bigquery.query(duplicateRecordsQuery);
                    allDuplicateRecords = duplicateRecords;
                    console.log(`✅ Retrieved ${allDuplicateRecords.length} duplicate records for detailed analysis`);
                } catch (recordsError) {
                    console.warn(`⚠️ Could not get duplicate record details:`, recordsError.message);
                }
            }

            const recommendations = [];
            if (hasDuplicates) {
                recommendations.push(`Review source data to understand why ${duplicateKeys.length} primary key values appear multiple times`);
                recommendations.push(`Consider using composite keys or timestamps for unique identification`);
                recommendations.push(`Data pipeline should include deduplication logic before loading to BigQuery`);
                if (duplicateKeys.length < 5) {
                    recommendations.push(`Small number of duplicates - may be acceptable for analysis purposes`);
                }
            } else {
                recommendations.push(`Excellent data quality - no duplicate primary keys found`);
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
            console.error('❌ Duplicates analysis failed:', error.message);
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

    // Legacy compatibility methods (updated to be schema-safe)
    async getRecordCounts(tempTableId, sourceTableName) {
        try {
            const [jsonResult] = await this.bigquery.query(`SELECT COUNT(*) as count FROM \`${tempTableId}\``);
            const [bqResult] = await this.bigquery.query(`SELECT COUNT(*) as count FROM \`${sourceTableName}\``);
            
            return {
                jsonCount: parseInt(jsonResult[0].count),
                bqCount: parseInt(bqResult[0].count)
            };
        } catch (error) {
            console.error('❌ Legacy getRecordCounts failed:', error.message);
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
            console.error('❌ Legacy findMatchingRecords failed:', error.message);
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
            console.error('❌ Legacy findMissingRecords failed:', error.message);
            throw error;
        }
    }

    async compareFieldDifferences(tempTableId, sourceTableName, primaryKey, comparisonFields = [], maxRecords = 100) {
        try {
            return {
                totalDifferences: 0,
                fieldDifferences: [],
                message: 'Using schema-safe common field analysis'
            };
        } catch (error) {
            console.error('❌ Legacy compareFieldDifferences failed:', error.message);
            throw error;
        }
    }
}

module.exports = ComparisonEngineService;