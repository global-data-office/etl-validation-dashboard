// services/rdbms-integration.js
const { Pool } = require('pg'); // PostgreSQL
const mysql = require('mysql2/promise'); // MySQL
const oracledb = require('oracledb'); // Oracle
const sql = require('mssql'); // SQL Server
const { BigQuery } = require('@google-cloud/bigquery');

class RDBMSIntegrationService {
    constructor() {
        this.bigquery = new BigQuery({
            projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
        });
    }

    // Test database connections
    async testConnection(dbType, config) {
        console.log(`Testing ${dbType} connection...`);
        
        try {
            switch (dbType) {
                case 'postgresql':
                    return await this.testPostgreSQLConnection(config);
                case 'mysql':
                    return await this.testMySQLConnection(config);
                case 'oracle':
                    return await this.testOracleConnection(config);
                case 'sqlserver':
                    return await this.testSQLServerConnection(config);
                default:
                    throw new Error(`Unsupported database type: ${dbType}`);
            }
        } catch (error) {
            console.error(`${dbType} connection test failed:`, error.message);
            throw error;
        }
    }

    async testPostgreSQLConnection(config) {
        const pool = new Pool({
            host: config.host,
            port: config.port || 5432,
            database: config.database,
            user: config.username,
            password: config.password,
            max: 1,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 5000,
        });

        try {
            const client = await pool.connect();
            const result = await client.query('SELECT version() as version');
            client.release();
            await pool.end();
            
            return {
                success: true,
                message: 'PostgreSQL connection successful',
                version: result.rows[0].version,
                database: config.database
            };
        } catch (error) {
            await pool.end();
            throw new Error(`PostgreSQL connection failed: ${error.message}`);
        }
    }

    async testMySQLConnection(config) {
        const connection = await mysql.createConnection({
            host: config.host,
            port: config.port || 3306,
            user: config.username,
            password: config.password,
            database: config.database,
            connectTimeout: 5000,
            acquireTimeout: 5000
        });

        try {
            const [rows] = await connection.execute('SELECT VERSION() as version');
            await connection.end();
            
            return {
                success: true,
                message: 'MySQL connection successful',
                version: rows[0].version,
                database: config.database
            };
        } catch (error) {
            await connection.end();
            throw new Error(`MySQL connection failed: ${error.message}`);
        }
    }

    async testOracleConnection(config) {
        const connectionString = `${config.host}:${config.port || 1521}/${config.serviceName}`;
        
        let connection;
        try {
            connection = await oracledb.getConnection({
                user: config.username,
                password: config.password,
                connectString: connectionString,
                connectTimeout: 5
            });
            
            const result = await connection.execute('SELECT * FROM v$version WHERE rownum = 1');
            
            return {
                success: true,
                message: 'Oracle connection successful',
                version: result.rows[0][0],
                serviceName: config.serviceName
            };
        } catch (error) {
            throw new Error(`Oracle connection failed: ${error.message}`);
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    }

    async testSQLServerConnection(config) {
        const sqlConfig = {
            user: config.username,
            password: config.password,
            server: config.host,
            port: config.port || 1433,
            database: config.database,
            options: {
                encrypt: true,
                trustServerCertificate: true
            },
            connectionTimeout: 5000,
            requestTimeout: 5000
        };

        try {
            const pool = await sql.connect(sqlConfig);
            const result = await pool.request().query('SELECT @@VERSION as version');
            await pool.close();
            
            return {
                success: true,
                message: 'SQL Server connection successful',
                version: result.recordset[0].version,
                database: config.database
            };
        } catch (error) {
            throw new Error(`SQL Server connection failed: ${error.message}`);
        }
    }

    // Get table schema and sample data
    async getTableInfo(dbType, config, tableName) {
        console.log(`Getting table info for ${dbType}: ${tableName}`);
        
        switch (dbType) {
            case 'postgresql':
                return await this.getPostgreSQLTableInfo(config, tableName);
            case 'mysql':
                return await this.getMySQLTableInfo(config, tableName);
            case 'oracle':
                return await this.getOracleTableInfo(config, tableName);
            case 'sqlserver':
                return await this.getSQLServerTableInfo(config, tableName);
            default:
                throw new Error(`Unsupported database type: ${dbType}`);
        }
    }

    async getPostgreSQLTableInfo(config, tableName) {
        const pool = new Pool({
            host: config.host,
            port: config.port || 5432,
            database: config.database,
            user: config.username,
            password: config.password,
            max: 1
        });

        try {
            const client = await pool.connect();
            
            // Get table schema
            const schemaQuery = `
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = $1 
                ORDER BY ordinal_position
            `;
            const schema = await client.query(schemaQuery, [tableName.toLowerCase()]);
            
            // Get row count
            const countQuery = `SELECT COUNT(*) as count FROM ${tableName}`;
            const count = await client.query(countQuery);
            
            // Get sample data (first 5 rows)
            const sampleQuery = `SELECT * FROM ${tableName} LIMIT 5`;
            const sample = await client.query(sampleQuery);
            
            client.release();
            await pool.end();
            
            return {
                success: true,
                tableName: tableName,
                rowCount: parseInt(count.rows[0].count),
                columns: schema.rows,
                sampleData: sample.rows,
                fields: schema.rows.map(col => col.column_name)
            };
        } catch (error) {
            await pool.end();
            throw new Error(`PostgreSQL table info failed: ${error.message}`);
        }
    }

    async getMySQLTableInfo(config, tableName) {
        const connection = await mysql.createConnection({
            host: config.host,
            port: config.port || 3306,
            user: config.username,
            password: config.password,
            database: config.database
        });

        try {
            // Get table schema
            const [schema] = await connection.execute(`
                SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, IS_NULLABLE as is_nullable
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
                ORDER BY ORDINAL_POSITION
            `, [tableName, config.database]);
            
            // Get row count
            const [count] = await connection.execute(`SELECT COUNT(*) as count FROM ${tableName}`);
            
            // Get sample data
            const [sample] = await connection.execute(`SELECT * FROM ${tableName} LIMIT 5`);
            
            await connection.end();
            
            return {
                success: true,
                tableName: tableName,
                rowCount: count[0].count,
                columns: schema,
                sampleData: sample,
                fields: schema.map(col => col.column_name)
            };
        } catch (error) {
            await connection.end();
            throw new Error(`MySQL table info failed: ${error.message}`);
        }
    }

    async getOracleTableInfo(config, tableName) {
        const connectionString = `${config.host}:${config.port || 1521}/${config.serviceName}`;
        
        let connection;
        try {
            connection = await oracledb.getConnection({
                user: config.username,
                password: config.password,
                connectString: connectionString
            });
            
            // Get table schema
            const schemaResult = await connection.execute(`
                SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
                FROM ALL_TAB_COLUMNS 
                WHERE TABLE_NAME = UPPER(:tableName)
                ORDER BY COLUMN_ID
            `, { tableName: tableName.toUpperCase() });
            
            // Get row count
            const countResult = await connection.execute(`SELECT COUNT(*) as COUNT FROM ${tableName}`);
            
            // Get sample data
            const sampleResult = await connection.execute(
                `SELECT * FROM ${tableName} WHERE ROWNUM <= 5`
            );
            
            const columns = schemaResult.rows.map(row => ({
                column_name: row[0],
                data_type: row[1],
                is_nullable: row[2] === 'Y' ? 'YES' : 'NO'
            }));
            
            return {
                success: true,
                tableName: tableName,
                rowCount: countResult.rows[0][0],
                columns: columns,
                sampleData: sampleResult.rows,
                fields: columns.map(col => col.column_name)
            };
        } catch (error) {
            throw new Error(`Oracle table info failed: ${error.message}`);
        } finally {
            if (connection) {
                await connection.close();
            }
        }
    }

    async getSQLServerTableInfo(config, tableName) {
        const sqlConfig = {
            user: config.username,
            password: config.password,
            server: config.host,
            port: config.port || 1433,
            database: config.database,
            options: {
                encrypt: true,
                trustServerCertificate: true
            }
        };

        try {
            const pool = await sql.connect(sqlConfig);
            
            // Get table schema
            const schemaResult = await pool.request().query(`
                SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, IS_NULLABLE as is_nullable
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '${tableName}'
                ORDER BY ORDINAL_POSITION
            `);
            
            // Get row count
            const countResult = await pool.request().query(`SELECT COUNT(*) as count FROM ${tableName}`);
            
            // Get sample data
            const sampleResult = await pool.request().query(`SELECT TOP 5 * FROM ${tableName}`);
            
            await pool.close();
            
            return {
                success: true,
                tableName: tableName,
                rowCount: countResult.recordset[0].count,
                columns: schemaResult.recordset,
                sampleData: sampleResult.recordset,
                fields: schemaResult.recordset.map(col => col.column_name)
            };
        } catch (error) {
            throw new Error(`SQL Server table info failed: ${error.message}`);
        }
    }

    // Compare RDBMS data with BigQuery
    async compareWithBigQuery(dbType, config, tableName, bqTable, primaryKey, comparisonFields = []) {
        console.log(`Starting ${dbType} vs BigQuery comparison...`);
        
        try {
            // Get RDBMS data
            const rdbmsData = await this.getRDBMSData(dbType, config, tableName, primaryKey, comparisonFields);
            
            // Get BigQuery data  
            const bqData = await this.getBigQueryData(bqTable, primaryKey, comparisonFields);
            
            // Perform comparison
            const comparison = this.performComparison(rdbmsData, bqData, primaryKey, comparisonFields);
            
            return {
                success: true,
                summary: {
                    sourceRecords: rdbmsData.rowCount,
                    targetRecords: bqData.rowCount,
                    matchingRecords: comparison.matchingRecords,
                    successRate: comparison.successRate,
                    fieldIssues: comparison.fieldIssues
                },
                metadata: {
                    sourceTable: tableName,
                    targetTable: bqTable,
                    primaryKey: primaryKey,
                    dbType: dbType
                },
                comparison: comparison
            };
        } catch (error) {
            console.error(`${dbType} vs BigQuery comparison failed:`, error.message);
            throw error;
        }
    }

    async getRDBMSData(dbType, config, tableName, primaryKey, fields) {
        const fieldsToSelect = fields.length > 0 ? fields.join(', ') : '*';
        let query;
        
        switch (dbType) {
            case 'postgresql':
            case 'mysql':
                query = `SELECT ${fieldsToSelect} FROM ${tableName}`;
                break;
            case 'oracle':
                query = `SELECT ${fieldsToSelect} FROM ${tableName}`;
                break;
            case 'sqlserver':
                query = `SELECT ${fieldsToSelect} FROM ${tableName}`;
                break;
        }
        
        // Get connection and execute query
        let connection, result;
        
        switch (dbType) {
            case 'postgresql':
                const pgPool = new Pool({
                    host: config.host,
                    port: config.port || 5432,
                    database: config.database,
                    user: config.username,
                    password: config.password,
                    max: 1
                });
                
                const client = await pgPool.connect();
                result = await client.query(query);
                client.release();
                await pgPool.end();
                
                return {
                    rows: result.rows,
                    rowCount: result.rows.length,
                    fields: Object.keys(result.rows[0] || {})
                };
                
            case 'mysql':
                connection = await mysql.createConnection({
                    host: config.host,
                    port: config.port || 3306,
                    user: config.username,
                    password: config.password,
                    database: config.database
                });
                
                const [rows] = await connection.execute(query);
                await connection.end();
                
                return {
                    rows: rows,
                    rowCount: rows.length,
                    fields: Object.keys(rows[0] || {})
                };
                
            case 'oracle':
                const connectionString = `${config.host}:${config.port || 1521}/${config.serviceName}`;
                
                connection = await oracledb.getConnection({
                    user: config.username,
                    password: config.password,
                    connectString: connectionString
                });
                
                result = await connection.execute(query);
                await connection.close();
                
                return {
                    rows: result.rows,
                    rowCount: result.rows.length,
                    fields: result.metaData.map(col => col.name)
                };
                
            case 'sqlserver':
                const sqlConfig = {
                    user: config.username,
                    password: config.password,
                    server: config.host,
                    port: config.port || 1433,
                    database: config.database,
                    options: {
                        encrypt: true,
                        trustServerCertificate: true
                    }
                };
                
                const pool = await sql.connect(sqlConfig);
                result = await pool.request().query(query);
                await pool.close();
                
                return {
                    rows: result.recordset,
                    rowCount: result.recordset.length,
                    fields: Object.keys(result.recordset[0] || {})
                };
                
            default:
                throw new Error(`Unsupported database type: ${dbType}`);
        }
    }

    async getBigQueryData(tableName, primaryKey, fields) {
        const fieldsToSelect = fields.length > 0 ? fields.join(', ') : '*';
        const query = `SELECT ${fieldsToSelect} FROM \`${tableName}\``;
        
        const [rows] = await this.bigquery.query(query);
        
        return {
            rows: rows,
            rowCount: rows.length,
            fields: Object.keys(rows[0] || {})
        };
    }

    performComparison(rdbmsData, bqData, primaryKey, comparisonFields) {
        const rdbmsMap = new Map();
        const bqMap = new Map();
        
        // Create lookup maps
        rdbmsData.rows.forEach(row => {
            rdbmsMap.set(String(row[primaryKey]), row);
        });
        
        bqData.rows.forEach(row => {
            bqMap.set(String(row[primaryKey]), row);
        });
        
        let matchingRecords = 0;
        let fieldIssues = 0;
        const missingInBQ = [];
        const missingInRDBMS = [];
        
        // Check RDBMS records in BigQuery
        for (const [key, rdbmsRow] of rdbmsMap) {
            if (bqMap.has(key)) {
                matchingRecords++;
                
                // Compare field values if specified
                if (comparisonFields.length > 0) {
                    const bqRow = bqMap.get(key);
                    comparisonFields.forEach(field => {
                        if (String(rdbmsRow[field]) !== String(bqRow[field])) {
                            fieldIssues++;
                        }
                    });
                }
            } else {
                missingInBQ.push(key);
            }
        }
        
        // Check for records in BigQuery but not in RDBMS
        for (const [key] of bqMap) {
            if (!rdbmsMap.has(key)) {
                missingInRDBMS.push(key);
            }
        }
        
        const successRate = rdbmsData.rowCount > 0 ? 
            Math.round((matchingRecords / rdbmsData.rowCount) * 100) : 0;
        
        return {
            matchingRecords,
            successRate,
            fieldIssues,
            missingInBQ: missingInBQ.length,
            missingInRDBMS: missingInRDBMS.length,
            missingInBQKeys: missingInBQ.slice(0, 10), // Sample
            missingInRDBMSKeys: missingInRDBMS.slice(0, 10) // Sample
        };
    }
}

module.exports = RDBMSIntegrationService;