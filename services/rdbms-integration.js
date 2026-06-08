// services/rdbms-integration.js - COMPLETE WORKING VERSION
const { Pool } = require('pg');
const mysql = require('mysql2/promise');
const oracledb = require('oracledb');
const sql = require('mssql');
const { BigQuery } = require('@google-cloud/bigquery');

// Enable Oracle thick mode
try {
    oracledb.initOracleClient({ 
        libDir: 'C:\\oracle\\instantclient_19_28'
    });
    console.log('✅ Oracle thick mode initialized successfully');
    console.log('📊 Oracle Client Version:', oracledb.oracleClientVersionString || 'Available');
} catch (err) {
    console.error('⚠️ Oracle thick mode initialization:', err.message);
    console.log('Continuing with thin mode - some Oracle features may be limited');
}

class RDBMSIntegrationService {
    constructor() {
        this.connections = new Map();
        this.bigquery = new BigQuery({
            // Uses Application Default Credentials from environment
        });
    }
	/**
    * Build Oracle connection string based on SID or Service Name
    * Users provide EITHER serviceName OR sid in config
    * 
    * IMPORTANT: Both formats now use forward slash (/) for Easy Connect syntax
    * - Service Name: host:port/servicename
    * - SID: host:port/sid
    */
    /**
 * Build Oracle connection string based on SID or Service Name
 * Uses TNS descriptor format for maximum compatibility with thick client
 */
buildOracleConnectString(config) {
    const host = config.host;
    const port = config.port || 1521;
    
    // Check which format user provided
    if (config.sid && config.sid.trim() !== '') {
        // TNS Descriptor format for SID (most compatible with thick client)
        const connectString = `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${host})(PORT=${port}))(CONNECT_DATA=(SID=${config.sid})))`;
        console.log('🔧 Oracle: Using TNS descriptor SID format ->', connectString);
        return connectString;
    } 
    else if (config.serviceName && config.serviceName.trim() !== '') {
        // TNS Descriptor format for Service Name
        const connectString = `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${host})(PORT=${port}))(CONNECT_DATA=(SERVICE_NAME=${config.serviceName})))`;
        console.log('🔧 Oracle: Using TNS descriptor Service Name format ->', connectString);
        return connectString;
    }
    // Fallback for backward compatibility with old 'service' field
    else if (config.service && config.service.trim() !== '') {
        const connectString = `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${host})(PORT=${port}))(CONNECT_DATA=(SERVICE_NAME=${config.service})))`;
        console.log('🔧 Oracle: Using TNS descriptor legacy service format ->', connectString);
        return connectString;
    }
    else {
        throw new Error('Oracle connection requires either Service Name or SID');
    }
}

    // MAIN METHOD: Test database connection
    async testConnection(dbType, connectionConfig) {
        try {
            console.log(`Testing ${dbType.toUpperCase()} connection...`);

            const validationResult = this.validateConnectionConfig(dbType, connectionConfig);
            if (!validationResult.valid) {
                return {
                    success: false,
                    error: validationResult.error,
                    suggestions: ['Please fill in all required connection fields']
                };
            }

            switch (dbType.toLowerCase()) {
                case 'postgresql':
                    return await this.testPostgreSQLConnection(connectionConfig);
                case 'mysql':
                    return await this.testMySQLConnection(connectionConfig);
                case 'oracle':
                    return await this.testOracleConnection(connectionConfig);
                case 'sqlserver':
                    return await this.testSQLServerConnection(connectionConfig);
                default:
                    throw new Error(`Unsupported database type: ${dbType}`);
            }
        } catch (error) {
            console.error(`${dbType} connection test failed:`, error.message);
            return {
                success: false,
                error: error.message,
                suggestions: this.getConnectionSuggestions(dbType, error.message)
            };
        }
    }

    // Validate connection config
    validateConnectionConfig(dbType, config) {
        const requiredFields = {
            postgresql: ['host', 'port', 'database', 'username', 'password'],
            mysql: ['host', 'port', 'database', 'username', 'password'],
            oracle: ['host', 'port', 'username', 'password'], // SID or serviceName checked separately
            sqlserver: ['server', 'port', 'database', 'username', 'password']
        };

        const required = requiredFields[dbType.toLowerCase()] || [];
        const missing = required.filter(field => !config[field] || config[field].toString().trim() === '');

        if (missing.length > 0) {
            return {
                valid: false,
                error: `Missing required fields: ${missing.join(', ')}`
            };
        }
    // Oracle-specific validation: require either SID or Service Name
        if (dbType.toLowerCase() === 'oracle') {
            const hasServiceName = config.serviceName && config.serviceName.trim() !== '';
            const hasSID = config.sid && config.sid.trim() !== '';
            
            if (!hasServiceName && !hasSID) {
                return {
                    valid: false,
                    error: 'Oracle connection requires either Service Name or SID'
                };
            }
        }

        return { valid: true };
    }

    // PostgreSQL connection test
    async testPostgreSQLConnection(config) {
        const pool = new Pool({
            host: config.host,
            port: parseInt(config.port) || 5432,
            database: config.database,
            user: config.username,
            password: config.password,
            connectionTimeoutMillis: 10000,
            idleTimeoutMillis: 5000,
            max: 1,
            ssl: false
        });

        try {
            const client = await pool.connect();
            
            const result = await client.query(`
                SELECT 
                    version() as version,
                    current_database() as database,
                    current_user as user,
                    inet_server_addr() as server_ip,
                    inet_server_port() as server_port
            `);
            
            client.release();
            await pool.end();

            return {
                success: true,
                message: 'PostgreSQL connection successful',
                details: {
                    version: result.rows[0].version.split(' ')[0] + ' ' + result.rows[0].version.split(' ')[1],
                    database: result.rows[0].database,
                    user: result.rows[0].user,
                    server_ip: result.rows[0].server_ip || config.host,
                    server_port: result.rows[0].server_port || config.port
                }
            };
        } catch (error) {
            await pool.end().catch(() => {});
            throw error;
        }
    }

    // MySQL connection test
    async testMySQLConnection(config) {
        const connection = await mysql.createConnection({
            host: config.host,
            port: parseInt(config.port) || 3306,
            database: config.database,
            user: config.username,
            password: config.password,
            connectTimeout: 10000,
            acquireTimeout: 10000,
            timeout: 10000,
            charset: 'utf8mb4'
        });

        try {
            const [rows] = await connection.execute(`
                SELECT 
                    VERSION() as version, 
                    DATABASE() as database, 
                    USER() as user,
                    @@hostname as hostname,
                    @@port as port
            `);
            
            await connection.end();

            return {
                success: true,
                message: 'MySQL connection successful',
                details: {
                    version: rows[0].version,
                    database: rows[0].database,
                    user: rows[0].user.split('@')[0],
                    hostname: rows[0].hostname,
                    port: rows[0].port
                }
            };
        } catch (error) {
            await connection.end().catch(() => {});
            throw error;
        }
    }

    // Oracle connection test
    async testOracleConnection(config) {
        console.log('🔍 Oracle connection attempt with thick mode...');
        console.log('📊 Oracle mode:', oracledb.thin ? 'Thin Mode' : 'Thick Mode');
        
        const connectionConfig = {
            user: config.username,
            password: config.password,
            connectString: this.buildOracleConnectString(config),
            connectTimeout: 15000,
            callTimeout: 15000
        };

        let connection;
        try {
            console.log('🔗 Connecting to:', connectionConfig.connectString);
            connection = await oracledb.getConnection(connectionConfig);
            console.log('✅ Oracle connection established');
            
            const result = await connection.execute(`
                SELECT 
                    banner as version,
                    SYS_CONTEXT('USERENV', 'DB_NAME') as database,
                    SYS_CONTEXT('USERENV', 'SESSION_USER') as current_user,
                    SYS_CONTEXT('USERENV', 'SERVER_HOST') as hostname
                FROM v$version 
                WHERE banner LIKE 'Oracle%' 
                AND ROWNUM = 1
            `);
            
            console.log('✅ Oracle test query successful');
            await connection.close();

            return {
                success: true,
                message: 'Oracle connection successful',
                details: {
                    version: result.rows[0] ? result.rows[0][0] : 'Oracle Database',
                    database: result.rows[0] ? result.rows[0][1] : config.service,
                    user: result.rows[0] ? result.rows[0][2] : config.username,
                    hostname: result.rows[0] ? result.rows[0][3] : config.host,
                    service: config.sid ? `SID: ${config.sid}` : `Service: ${config.serviceName}`,
                    clientMode: oracledb.thin ? 'Thin' : 'Thick',
                    clientVersion: oracledb.oracleClientVersionString || 'Unknown'
                }
            };
        } catch (error) {
            console.error('❌ Oracle connection failed:', error.message);
            if (connection) await connection.close().catch(() => {});
            throw error;
        }
    }

    // SQL Server connection test
    async testSQLServerConnection(config) {
        const connectionConfig = {
            server: config.server,
            port: parseInt(config.port) || 1433,
            database: config.database,
            user: config.username,
            password: config.password,
            options: {
                encrypt: config.encrypt !== false,
                trustServerCertificate: true,
                enableArithAbort: true
            },
            connectionTimeout: 10000,
            requestTimeout: 10000,
            pool: {
                max: 1,
                min: 0,
                idleTimeoutMillis: 5000
            }
        };

        try {
            const pool = await sql.connect(connectionConfig);
            
            const result = await pool.request().query(`
                SELECT 
                    @@VERSION as version,
                    DB_NAME() as database,
                    SYSTEM_USER as current_user,
                    @@SERVERNAME as server_name,
                    @@SERVICENAME as service_name
            `);
            
            await pool.close();

            return {
                success: true,
                message: 'SQL Server connection successful',
                details: {
                    version: result.recordset[0].version.split('\n')[0].trim(),
                    database: result.recordset[0].database,
                    user: result.recordset[0].current_user,
                    server_name: result.recordset[0].server_name,
                    service_name: result.recordset[0].service_name
                }
            };
        } catch (error) {
            await sql.close().catch(() => {});
            throw error;
        }
    }

    // Enhanced error suggestions
    getConnectionSuggestions(dbType, errorMessage) {
        const suggestions = [];
        const error = errorMessage.toLowerCase();

        // General connection issues
        if (error.includes('timeout') || error.includes('connect')) {
            suggestions.push('✓ Check if the database server is running and accessible');
            suggestions.push('✓ Verify the host and port are correct');
            suggestions.push('✓ Check firewall settings and network connectivity');
        }

        if (error.includes('authentication') || error.includes('password') || error.includes('login')) {
            suggestions.push('✓ Verify username and password are correct');
            suggestions.push('✓ Check if the user has proper database permissions');
        }

        // Database-specific suggestions
        switch (dbType.toLowerCase()) {
            case 'oracle':
                if (error.includes('njs-533') || error.includes('encryption')) {
                    suggestions.push('🔴 Oracle: Thick mode enabled to support advanced networking');
                    suggestions.push('🔴 Oracle: Check if Oracle Client libraries are properly installed');
                }
                break;
        }

        return suggestions.length > 0 ? suggestions : ['Check connection parameters and try again'];
    }

    // Parse BigQuery table name (project.dataset.table)
    parseBigQueryTable(bqTableName) {
        if (!bqTableName) {
            throw new Error('BigQuery table name is required');
        }

        const parts = bqTableName.split('.');
        if (parts.length !== 3) {
            throw new Error('BigQuery table must be in format: project.dataset.table');
        }

        return {
            projectId: parts[0],
            datasetId: parts[1],
            tableId: parts[2]
        };
    }

    // NEW: Compare RDBMS with BigQuery
    async compareWithBigQuery(dbType, connectionConfig, sourceTable, bqTableName, primaryKey, comparisonFields = []) {
        try {
            console.log(`🔄 Starting ${dbType.toUpperCase()} vs BigQuery comparison...`);
            
            // Parse BigQuery table
            const bqTable = this.parseBigQueryTable(bqTableName);
            console.log(`📊 Source: ${dbType} - ${sourceTable}`);
            console.log(`📊 Target: BigQuery - ${bqTable.projectId}.${bqTable.datasetId}.${bqTable.tableId}`);

            // Step 1: Get data from source RDBMS
            console.log('🔍 Fetching data from source database...');
            const sourceData = await this.fetchSourceData(dbType, connectionConfig, sourceTable, primaryKey, comparisonFields);
            
            // Step 2: Get data from BigQuery
            console.log('🔍 Fetching data from BigQuery...');
            const bqData = await this.fetchBigQueryData(bqTable, primaryKey, comparisonFields);

            // Step 3: Compare data
            console.log('🔄 Comparing data...');
            const comparison = await this.performDataComparison(sourceData, bqData, primaryKey, comparisonFields);

            console.log('✅ Comparison completed successfully');
            return {
                success: true,
                comparison: comparison,
                summary: {
                    sourceType: dbType.toUpperCase(),
                    sourceTable: sourceTable,
                    bqTable: bqTableName,
                    primaryKey: primaryKey,
                    comparisonFields: comparisonFields,
                    sourceRecords: sourceData.recordCount,
                    bqRecords: bqData.recordCount,
                    matchingRecords: comparison.matchingRecords,
                    differences: comparison.differences,
                    successRate: ((comparison.matchingRecords / Math.max(sourceData.recordCount, bqData.recordCount)) * 100).toFixed(2) + '%'
                }
            };

        } catch (error) {
            console.error(`${dbType} vs BigQuery comparison failed:`, error.message);
            throw new Error(`Comparison failed: ${error.message}`);
        }
    }

    // Fetch data from source RDBMS
	async fetchSourceData(dbType, connectionConfig, tableName, primaryKey, comparisonFields) {
    try {
        const fields = [primaryKey, ...comparisonFields].join(', ');
        let query;
        
        // Database-specific query syntax
        switch (dbType.toLowerCase()) {
            case 'oracle':
                // Oracle uses ROWNUM instead of LIMIT
                query = `SELECT ${fields} FROM ${tableName} WHERE ROWNUM <= 1000 ORDER BY ${primaryKey}`;
                break;
            case 'postgresql':
            case 'mysql':
                // PostgreSQL and MySQL use LIMIT
                query = `SELECT ${fields} FROM ${tableName} ORDER BY ${primaryKey} LIMIT 1000`;
                break;
            case 'sqlserver':
                // SQL Server uses TOP
                query = `SELECT TOP 1000 ${fields} FROM ${tableName} ORDER BY ${primaryKey}`;
                break;
            default:
                // Default to LIMIT syntax
                query = `SELECT ${fields} FROM ${tableName} ORDER BY ${primaryKey} LIMIT 1000`;
        }
        
        console.log(`📋 ${dbType.toUpperCase()} query: ${query}`);

        switch (dbType.toLowerCase()) {
            case 'oracle':
                return await this.fetchOracleData(connectionConfig, query);
            case 'postgresql':
                return await this.fetchPostgreSQLData(connectionConfig, query);
            case 'mysql':
                return await this.fetchMySQLData(connectionConfig, query);
            case 'sqlserver':
                return await this.fetchSQLServerData(connectionConfig, query);
            default:
                throw new Error(`Unsupported database type: ${dbType}`);
        }
    } catch (error) {
        console.error(`Failed to fetch ${dbType} data:`, error.message);
        throw error;
        }
    }
    // Fetch Oracle data
    async fetchOracleData(config, query) {
        let connection;
        try {
            connection = await oracledb.getConnection({
                user: config.username,
                password: config.password,
                connectString: this.buildOracleConnectString(config),
                connectTimeout: 15000
            });

            const result = await connection.execute(query);
            await connection.close();

            return {
                records: result.rows.map(row => {
                    const record = {};
                    result.metaData.forEach((col, index) => {
                        record[col.name] = row[index];
                    });
                    return record;
                }),
                recordCount: result.rows.length
            };
        } catch (error) {
            if (connection) await connection.close().catch(() => {});
            throw error;
        }
    }

    // Fetch PostgreSQL data
    async fetchPostgreSQLData(config, query) {
        const pool = new Pool({
            host: config.host,
            port: parseInt(config.port) || 5432,
            database: config.database,
            user: config.username,
            password: config.password,
            connectionTimeoutMillis: 10000,
            max: 1,
            ssl: false
        });

        try {
            const client = await pool.connect();
            const result = await client.query(query);
            client.release();
            await pool.end();

            return {
                records: result.rows,
                recordCount: result.rows.length
            };
        } catch (error) {
            await pool.end().catch(() => {});
            throw error;
        }
    }

    // Fetch MySQL data
    async fetchMySQLData(config, query) {
        const connection = await mysql.createConnection({
            host: config.host,
            port: parseInt(config.port) || 3306,
            database: config.database,
            user: config.username,
            password: config.password,
            connectTimeout: 10000
        });

        try {
            const [rows] = await connection.execute(query);
            await connection.end();
            
            return {
                records: rows,
                recordCount: rows.length
            };
        } catch (error) {
            await connection.end().catch(() => {});
            throw error;
        }
    }

    // Fetch SQL Server data
    async fetchSQLServerData(config, query) {
        try {
            const pool = await sql.connect({
                server: config.server,
                port: parseInt(config.port) || 1433,
                database: config.database,
                user: config.username,
                password: config.password,
                options: {
                    encrypt: true,
                    trustServerCertificate: true
                },
                connectionTimeout: 10000,
                pool: { max: 1, min: 0 }
            });

            const result = await pool.request().query(query);
            await pool.close();

            return {
                records: result.recordset,
                recordCount: result.recordset.length
            };
        } catch (error) {
            await sql.close().catch(() => {});
            throw error;
        }
    }

    // Fetch BigQuery data
    async fetchBigQueryData(bqTable, primaryKey, comparisonFields) {
        try {
            const fields = [primaryKey, ...comparisonFields].join(', ');
            const query = `
                SELECT ${fields}
                FROM \`${bqTable.projectId}.${bqTable.datasetId}.${bqTable.tableId}\`
                ORDER BY ${primaryKey}
                LIMIT 1000
            `;
            
            console.log(`📋 BigQuery query: ${query}`);

            const [job] = await this.bigquery.createQueryJob({
                query: query,
                location: 'US',
            });

            const [rows] = await job.getQueryResults();
            
            return {
                records: rows,
                recordCount: rows.length,
                query: query
            };
        } catch (error) {
            console.error('Failed to fetch BigQuery data:', error.message);
            throw new Error(`BigQuery fetch failed: ${error.message}`);
        }
    }

    // Perform data comparison
    async performDataComparison(sourceData, bqData, primaryKey, comparisonFields) {
        try {
            const sourceMap = new Map();
            const bqMap = new Map();
            
            // Create maps for efficient lookups
            sourceData.records.forEach(record => {
                sourceMap.set(String(record[primaryKey]), record);
            });
            
            bqData.records.forEach(record => {
                bqMap.set(String(record[primaryKey]), record);
            });

            let matchingRecords = 0;
            const differences = [];
            const allKeys = new Set([...sourceMap.keys(), ...bqMap.keys()]);

            for (const key of allKeys) {
                const sourceRecord = sourceMap.get(key);
                const bqRecord = bqMap.get(key);
                
                if (!sourceRecord) {
                    differences.push({
                        primaryKey: key,
                        issue: 'Missing in source',
                        sourceValue: null,
                        bqValue: bqRecord
                    });
                } else if (!bqRecord) {
                    differences.push({
                        primaryKey: key,
                        issue: 'Missing in BigQuery',
                        sourceValue: sourceRecord,
                        bqValue: null
                    });
                } else {
                    // Compare field values
                    let recordMatches = true;
                    const fieldDifferences = [];
                    
                    for (const field of comparisonFields) {
                        const sourceVal = String(sourceRecord[field] || '');
                        const bqVal = String(bqRecord[field] || '');
                        
                        if (sourceVal !== bqVal) {
                            recordMatches = false;
                            fieldDifferences.push({
                                field: field,
                                sourceValue: sourceVal,
                                bqValue: bqVal
                            });
                        }
                    }
                    
                    if (recordMatches) {
                        matchingRecords++;
                    } else {
                        differences.push({
                            primaryKey: key,
                            issue: 'Field differences',
                            fieldDifferences: fieldDifferences
                        });
                    }
                }
            }

            return {
                matchingRecords,
                differences: differences.slice(0, 100), // Limit to first 100 differences
                totalDifferences: differences.length,
                comparisonFields
            };

        } catch (error) {
            console.error('Data comparison failed:', error.message);
            throw error;
        }
    }

    // Schema analysis placeholder methods
    async getSchemaInfo(dbType, connectionConfig, tableName) {
        return {
            tableName: tableName,
            schema: [],
            rowCount: 0,
            message: `${dbType} schema analysis - placeholder implementation`
        };
    }
}

module.exports = new RDBMSIntegrationService();