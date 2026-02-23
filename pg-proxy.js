const express = require('express');
const cors = require('cors');
const { Client } = require('pg');

const app = express();
app.use(cors());  // Allow requests from Windows
app.use(express.json());

// Test connection endpoint
app.post('/test-connection', async (req, res) => {
    const { host, port, database, username, password } = req.body;

    const client = new Client({
        host: host,
        port: parseInt(port) || 5432,
        database: database,
        user: username,
        password: password
    });

    try {
        await client.connect();
        const result = await client.query(`
            SELECT version() as version,
                   current_database() as database,
                   current_user as user
        `);
        await client.end();

        res.json({
            success: true,
            message: 'PostgreSQL connection successful',
            details: {
                version: result.rows[0].version.split(',')[0],
                database: result.rows[0].database,
                user: result.rows[0].user
            }
        });
    } catch (error) {
        await client.end().catch(() => {});
        res.json({ success: false, error: error.message });
    }
});

// Query endpoint
app.post('/query', async (req, res) => {
    const { host, port, database, username, password, sql } = req.body;

    const client = new Client({
        host: host,
        port: parseInt(port) || 5432,
        database: database,
        user: username,
        password: password
    });

    try {
        await client.connect();
        const result = await client.query(sql);
        await client.end();

        res.json({
            success: true,
            records: result.rows,
            recordCount: result.rows.length
        });
    } catch (error) {
        await client.end().catch(() => {});
        res.json({ success: false, error: error.message });
    }
});

// Fetch schemas
app.post('/schemas', async (req, res) => {
    const { host, port, database, username, password } = req.body;

    const client = new Client({
        host: host,
        port: parseInt(port) || 5432,
        database: database,
        user: username,
        password: password
    });

    try {
        await client.connect();
        const result = await client.query(`
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        `);
        await client.end();

        res.json({
            success: true,
            schemas: result.rows.map(r => r.schema_name)
        });
    } catch (error) {
        await client.end().catch(() => {});
        res.json({ success: false, error: error.message });
    }
});

// Fetch tables for a schema
app.post('/tables', async (req, res) => {
    const { host, port, database, username, password, schema } = req.body;

    const client = new Client({
        host: host,
        port: parseInt(port) || 5432,
        database: database,
        user: username,
        password: password
    });

    try {
        await client.connect();
        const result = await client.query(`
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            ORDER BY table_name
        `, [schema]);
        await client.end();

        res.json({
            success: true,
            tables: result.rows.map(r => r.table_name)
        });
    } catch (error) {
        await client.end().catch(() => {});
        res.json({ success: false, error: error.message });
    }
});

const PORT = 3001;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`PostgreSQL Proxy running on port ${PORT}`);
});