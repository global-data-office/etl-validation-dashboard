// services/api-fetcher.js - Simplified API data fetching service
const https = require('https');
const http = require('http');
const { URL } = require('url');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');

class APIFetcherService {
    constructor() {
        this.tempDir = 'temp-api-data';
        this.timeoutMs = 30000;

        console.log('API Fetcher Service initialized (using built-in modules)');
        this.ensureTempDirectory();
    }

    async ensureTempDirectory() {
        try {
            await fs.access(this.tempDir);
        } catch {
            await fs.mkdir(this.tempDir, { recursive: true });
            console.log(`Created temp directory: ${this.tempDir}`);
        }
    }

    /**
     * Fetch data from API endpoint using built-in modules
     */
    async fetchAPIData(config) {
        const {
            url,
            method = 'GET',
            headers = {},
            body = null,
            authType = 'none',
            authCredentials = {},
            maxRecords = 10000
        } = config;

        console.log(`Fetching API data from: ${url}`);
        console.log(`Method: ${method}, Auth: ${authType}`);

        try {
            const urlObj = new URL(url);
            const isHttps = urlObj.protocol === 'https:';
            const httpModule = isHttps ? https : http;

            const requestOptions = {
                hostname: urlObj.hostname,
                port: urlObj.port || (isHttps ? 443 : 80),
                path: urlObj.pathname + urlObj.search,
                method: method.toUpperCase(),
                headers: {
                    'User-Agent': 'ETL-Validation-Dashboard/1.0',
                    'Accept': 'application/json',
                    ...headers
                },
                timeout: this.timeoutMs
            };

            // Handle authentication
            if (authType === 'bearer' && authCredentials.token) {
                requestOptions.headers['Authorization'] = `Bearer ${authCredentials.token}`;
            } else if (authType === 'basic' && authCredentials.username && authCredentials.password) {
                const credentials = Buffer.from(`${authCredentials.username}:${authCredentials.password}`).toString('base64');
                requestOptions.headers['Authorization'] = `Basic ${credentials}`;
            } else if (authType === 'apikey' && authCredentials.key && authCredentials.value) {
                requestOptions.headers[authCredentials.key] = authCredentials.value;
            }

            let requestBody = null;
            if (method.toUpperCase() === 'POST' && body) {
                requestBody = JSON.stringify(body);
                requestOptions.headers['Content-Type'] = 'application/json';
                requestOptions.headers['Content-Length'] = Buffer.byteLength(requestBody);
            }

            const responseData = await this.makeHttpRequest(httpModule, requestOptions, requestBody);

            // Parse response
            let jsonData;
            try {
                jsonData = JSON.parse(responseData);
            } catch (parseError) {
                throw new Error(`API returned invalid JSON: ${parseError.message}`);
            }

            // Extract data if it's nested
            let finalData = jsonData;
            if (config.dataPath && config.dataPath.trim()) {
                finalData = this.extractDataByPath(jsonData, config.dataPath);
            }

            // Ensure we have an array
            if (!Array.isArray(finalData)) {
                finalData = [finalData];
            }

            // Limit records
            if (finalData.length > maxRecords) {
                finalData = finalData.slice(0, maxRecords);
            }

            const cleanedData = this.cleanAPIData(finalData);

            return {
                success: true,
                data: cleanedData,
                metadata: {
                    totalRecords: cleanedData.length,
                    source: url,
                    method: method,
                    authType: authType,
                    fetchedAt: new Date().toISOString()
                }
            };

        } catch (error) {
            console.error('API fetch failed:', error.message);

            let errorMessage = error.message;
            let suggestions = [
                'Verify the API URL is correct and accessible',
                'Check authentication credentials if required',
                'Ensure the API returns JSON data'
            ];

            if (error.code === 'ECONNREFUSED') {
                suggestions = [
                    'API server is not responding or is down',
                    'Check if you can access the API in a browser',
                    'Verify network connectivity'
                ];
            } else if (error.code === 'ETIMEDOUT') {
                suggestions = [
                    'Request timed out - API may be slow',
                    'Try with a smaller dataset',
                    'Contact API provider about performance'
                ];
            }

            return {
                success: false,
                error: errorMessage,
                suggestions: suggestions
            };
        }
    }

    /**
     * Make HTTP request using built-in modules
     */
    makeHttpRequest(httpModule, options, body) {
        return new Promise((resolve, reject) => {
            const req = httpModule.request(options, (res) => {
                let data = '';

                res.on('data', (chunk) => {
                    data += chunk;
                });

                res.on('end', () => {
                    if (res.statusCode >= 200 && res.statusCode < 300) {
                        resolve(data);
                    } else {
                        reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
                    }
                });
            });

            req.on('error', (error) => {
                reject(error);
            });

            req.on('timeout', () => {
                req.destroy();
                reject(new Error('Request timed out'));
            });

            if (body) {
                req.write(body);
            }

            req.end();
        });
    }

    /**
     * Extract data from response using simple path
     */
    extractDataByPath(data, path) {
        if (!path || path === '.') return data;

        const parts = path.split('.');
        let current = data;

        for (const part of parts) {
            if (part === '') continue;
            if (current && typeof current === 'object' && part in current) {
                current = current[part];
            } else {
                console.warn(`Path '${path}' not found in response data`);
                return data;
            }
        }

        return current;
    }

    /**
     * Clean API data
     */
    cleanAPIData(rawData) {
        if (!Array.isArray(rawData)) {
            rawData = [rawData];
        }

        return rawData.map((record, index) => {
            if (typeof record !== 'object' || record === null) {
                console.warn(`Record ${index} is not an object, skipping`);
                return null;
            }

            return this.flattenObject(record);
        }).filter(record => record !== null);
    }

    /**
     * Flatten nested objects
     */
    flattenObject(obj, prefix = '', maxDepth = 5, currentDepth = 0) {
        const flattened = {};

        if (currentDepth >= maxDepth) {
            return { [prefix || 'data']: JSON.stringify(obj) };
        }

        for (const [key, value] of Object.entries(obj)) {
            const cleanKey = this.cleanFieldName(prefix ? `${prefix}_${key}` : key);

            if (value === null || value === undefined) {
                flattened[cleanKey] = null;
            } else if (Array.isArray(value)) {
                flattened[cleanKey] = JSON.stringify(value);
            } else if (typeof value === 'object') {
                Object.assign(flattened, this.flattenObject(value, cleanKey, maxDepth, currentDepth + 1));
            } else {
                flattened[cleanKey] = this.cleanDataValue(value, cleanKey);
            }
        }

        return flattened;
    }

    /**
     * Clean field names
     */
    cleanFieldName(fieldName) {
        return fieldName
            .replace(/[^a-zA-Z0-9_]/g, '_')
            .replace(/^[0-9]/, '_$&')
            .substring(0, 128);
    }

    /**
     * Clean data values
     */
    cleanDataValue(value, fieldName) {
        if (value === null || value === undefined) {
            return null;
        }

        let cleanValue = String(value).trim();

        if (cleanValue.length > 50000) {
            cleanValue = cleanValue.substring(0, 50000) + '... [TRUNCATED]';
        }

        return cleanValue
            .replace(/\0/g, '')
            .replace(/\r\n/g, '\n')
            .replace(/\r/g, '\n')
            .replace(/[\x00-\x1F\x7F]/g, ' ');
    }

    /**
     * Save API data to file
     */
    async saveAPIDataToFile(data, metadata) {
        const fileId = uuidv4();
        const fileName = `api_data_${fileId}.json`;
        const filePath = path.join(this.tempDir, fileName);

        // Ensure directory exists
        await this.ensureTempDirectory();

        await fs.writeFile(filePath, JSON.stringify(data, null, 2));
        console.log(`API data saved to: ${filePath}`);

        return {
            fileId: fileId,
            fileName: fileName,
            filePath: filePath,
            recordCount: data.length
        };
    }

    /**
     * Get preview of API data
     */
    getDataPreview(data, limit = 5) {
        if (!Array.isArray(data) || data.length === 0) {
            return {
                sampleRecords: [],
                totalRecords: 0,
                fieldsDetected: 0,
                availableFields: [],
                idFields: [],
                importantFields: []
            };
        }

        const sampleRecords = data.slice(0, limit);
        const firstRecord = data[0];
        const availableFields = Object.keys(firstRecord);

        const idFields = availableFields.filter(field => {
            const lowerField = field.toLowerCase();
            return lowerField.includes('id') ||
                   lowerField.includes('key') ||
                   lowerField.includes('number') ||
                   lowerField === 'uuid' ||
                   lowerField.endsWith('_id');
        });

        const importantFields = availableFields.filter(field => {
            const lowerField = field.toLowerCase();
            return !idFields.includes(field) && (
                lowerField.includes('name') ||
                lowerField.includes('title') ||
                lowerField.includes('email') ||
                lowerField.includes('status') ||
                lowerField.includes('type') ||
                lowerField.includes('date') ||
                lowerField.includes('time')
            );
        });

        return {
            sampleRecords: sampleRecords,
            totalRecords: data.length,
            fieldsDetected: availableFields.length,
            availableFields: availableFields,
            idFields: idFields,
            importantFields: importantFields
        };
    }

    /**
     * Test API connection
     */
    async testAPIConnection(config) {
        try {
            console.log(`Testing API connection to: ${config.url}`);

            const testConfig = {
                ...config,
                maxRecords: 1
            };

            const result = await this.fetchAPIData(testConfig);

            if (result.success) {
                return {
                    success: true,
                    message: 'API connection successful',
                    recordsFound: result.data.length,
                    fieldsDetected: result.data.length > 0 ? Object.keys(result.data[0]).length : 0
                };
            } else {
                return result;
            }
        } catch (error) {
            return {
                success: false,
                error: error.message,
                suggestions: ['Check API URL and configuration']
            };
        }
    }
}

module.exports = APIFetcherService;