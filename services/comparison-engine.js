// services/comparison-engine.js
const { BigQuery } = require('@google-cloud/bigquery');

class ComparisonEngineService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
        
        console.log('🔍 Comparison Engine Service initialized');
    }

    /**
     * Compare JSON temp table vs BigQuery source table
     */
    async compareJSONvsBigQuery(tempTableId, sourceTableName, primaryKey = 'sys_id', comparisonFields = []) {
        try {
            console.log('🚀 Starting JSON vs BigQuery comparison...');
            console.log(`📊 Temp table: ${tempTableId}`);
            console.log(`📋 Source table: ${sourceTableName}`);
            console.log(`🔑 Primary key: ${primaryKey}`);
            console.log(`🔧 Comparison fields: ${comparisonFields.length > 0 ? comparisonFields.join(', ') : 'All fields'}`);

            // Step 1: Get record counts
            const counts = await this.getRecordCounts(tempTableId, sourceTableName);
            console.log(`📊 JSON records: ${counts.jsonCount}, BQ records: ${counts.bqCount}`);

            // Step 2: Find matching records by primary key
            const matches = await this.findMatchingRecords(tempTableId, sourceTableName, primaryKey);
            console.log(`🔗 Matching records: ${matches.matchCount}`);

            // Step 3: Find missing records
            const missing = await this.findMissingRecords(tempTableId, sourceTableName, primaryKey);
            console.log(`❌ Missing from BQ: ${missing.missingFromBQ.length}, Missing from JSON: ${missing.missingFromJSON.length}`);

            // Step 4: Compare field differences for matching records
            const fieldDifferences = await this.compareFieldDifferences(
                tempTableId, 
                sourceTableName, 
                primaryKey, 
                comparisonFields,
                Math.min(matches.matchCount, 100) // Limit to first 100 matches for performance
            );
            console.log(`🔍 Field differences found: ${fieldDifferences.totalDifferences}`);

            // Step 5: Generate summary statistics
            const summary = {
                totalJSONRecords: counts.jsonCount,
                totalBQRecords: counts.bqCount,
                matchingRecords: matches.matchCount,
                missingFromBQ: missing.missingFromBQ.length,
                missingFromJSON: missing.missingFromJSON.length,
                matchRate: counts.jsonCount > 0 ? ((matches.matchCount / counts.jsonCount) * 100).toFixed(1) : '0.0',
                fieldDifferences: fieldDifferences.totalDifferences,
                comparisonDate: new Date().toISOString()
            };

            console.log('✅ Comparison completed successfully');

            return {
                success: true,
                summary: summary,
                matches: matches,
                missing: missing,
                fieldDifferences: fieldDifferences,
                metadata: {
                    tempTableId,
                    sourceTableName,
                    primaryKey,
                    comparisonFields: comparisonFields.length > 0 ? comparisonFields : ['all'],
                    comparisonDate: new Date().toISOString()
                }
            };

        } catch (error) {
            console.error('❌ Comparison failed:', error.message);
            throw new Error(`Comparison failed: ${error.message}`);
        }
    }

    /**
     * Get record counts from both tables
     */
    async getRecordCounts(tempTableId, sourceTableName) {
        try {
            console.log('📊 Getting record counts...');

            const queries = [
                `SELECT COUNT(*) as count FROM \`${tempTableId}\``,
                `SELECT COUNT(*) as count FROM \`${sourceTableName}\``
            ];

            const [jsonResult, bqResult] = await Promise.all(
                queries.map(query => this.bigquery.query(query))
            );

            return {
                jsonCount: parseInt(jsonResult[0][0].count),
                bqCount: parseInt(bqResult[0][0].count)
            };

        } catch (error) {
            console.error('❌ Failed to get record counts:', error.message);
            throw error;
        }
    }

    /**
     * Find matching records by primary key
     */
    async findMatchingRecords(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`🔗 Finding matching records by ${primaryKey}...`);

            const matchQuery = `
                SELECT COUNT(*) as match_count
                FROM \`${tempTableId}\` json_data
                INNER JOIN \`${sourceTableName}\` bq_data
                ON json_data.${primaryKey} = bq_data.${primaryKey}
                WHERE json_data.${primaryKey} IS NOT NULL
                  AND bq_data.${primaryKey} IS NOT NULL
            `;

            const [matchResult] = await this.bigquery.query(matchQuery);
            const matchCount = parseInt(matchResult[0].match_count);

            // Get sample of matching records
            const sampleQuery = `
                SELECT 
                    json_data.${primaryKey},
                    json_data.name as json_name,
                    bq_data.name as bq_name,
                    json_data.u_tenant_id as json_tenant_id,
                    bq_data.u_tenant_id as bq_tenant_id
                FROM \`${tempTableId}\` json_data
                INNER JOIN \`${sourceTableName}\` bq_data
                ON json_data.${primaryKey} = bq_data.${primaryKey}
                WHERE json_data.${primaryKey} IS NOT NULL
                  AND bq_data.${primaryKey} IS NOT NULL
                LIMIT 5
            `;

            const [sampleResult] = await this.bigquery.query(sampleQuery);

            return {
                matchCount: matchCount,
                sampleMatches: sampleResult
            };

        } catch (error) {
            console.error('❌ Failed to find matching records:', error.message);
            throw error;
        }
    }

    /**
     * Find missing records in both directions
     */
    async findMissingRecords(tempTableId, sourceTableName, primaryKey) {
        try {
            console.log(`❌ Finding missing records...`);

            // Records in JSON but not in BigQuery
            const missingFromBQQuery = `
                SELECT ${primaryKey}, name, u_tenant_id
                FROM \`${tempTableId}\` json_data
                WHERE json_data.${primaryKey} IS NOT NULL
                  AND json_data.${primaryKey} NOT IN (
                    SELECT ${primaryKey} 
                    FROM \`${sourceTableName}\` 
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;

            // Records in BigQuery but not in JSON
            const missingFromJSONQuery = `
                SELECT ${primaryKey}, name, u_tenant_id
                FROM \`${sourceTableName}\` bq_data
                WHERE bq_data.${primaryKey} IS NOT NULL
                  AND bq_data.${primaryKey} NOT IN (
                    SELECT ${primaryKey} 
                    FROM \`${tempTableId}\` 
                    WHERE ${primaryKey} IS NOT NULL
                  )
                LIMIT 10
            `;

            const [missingFromBQResult, missingFromJSONResult] = await Promise.all([
                this.bigquery.query(missingFromBQQuery),
                this.bigquery.query(missingFromJSONQuery)
            ]);

            return {
                missingFromBQ: missingFromBQResult[0],
                missingFromJSON: missingFromJSONResult[0]
            };

        } catch (error) {
            console.error('❌ Failed to find missing records:', error.message);
            throw error;
        }
    }

    /**
     * Compare field differences for matching records
     */
    async compareFieldDifferences(tempTableId, sourceTableName, primaryKey, comparisonFields, limit = 100) {
        try {
            console.log(`🔍 Comparing field differences (limit: ${limit})...`);

            // Define default fields to compare if none specified
            const fieldsToCompare = comparisonFields.length > 0 
                ? comparisonFields 
                : ['name', 'u_tenant_id', 'u_account_type', 'u_status'];

            console.log(`🔧 Comparing fields: ${fieldsToCompare.join(', ')}`);

            const differences = [];
            let totalDifferences = 0;

            for (const field of fieldsToCompare) {
                try {
                    const diffQuery = `
                        SELECT 
                            json_data.${primaryKey},
                            json_data.${field} as json_value,
                            bq_data.${field} as bq_value
                        FROM \`${tempTableId}\` json_data
                        INNER JOIN \`${sourceTableName}\` bq_data
                        ON json_data.${primaryKey} = bq_data.${primaryKey}
                        WHERE json_data.${primaryKey} IS NOT NULL
                          AND bq_data.${primaryKey} IS NOT NULL
                          AND (
                            (json_data.${field} IS NULL AND bq_data.${field} IS NOT NULL) OR
                            (json_data.${field} IS NOT NULL AND bq_data.${field} IS NULL) OR
                            (json_data.${field} != bq_data.${field})
                          )
                        LIMIT ${Math.min(limit, 20)}
                    `;

                    const [diffResult] = await this.bigquery.query(diffQuery);
                    
                    if (diffResult.length > 0) {
                        differences.push({
                            field: field,
                            differenceCount: diffResult.length,
                            sampleDifferences: diffResult.slice(0, 5)
                        });
                        totalDifferences += diffResult.length;
                        console.log(`   🔍 Field '${field}': ${diffResult.length} differences found`);
                    } else {
                        console.log(`   ✅ Field '${field}': No differences`);
                    }

                } catch (fieldError) {
                    console.warn(`   ⚠️ Could not compare field '${field}': ${fieldError.message}`);
                }
            }

            return {
                totalDifferences: totalDifferences,
                fieldDifferences: differences,
                fieldsCompared: fieldsToCompare
            };

        } catch (error) {
            console.error('❌ Failed to compare field differences:', error.message);
            throw error;
        }
    }
}

module.exports = ComparisonEngineService;