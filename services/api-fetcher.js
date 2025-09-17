// services/api-fetcher.js - COMPLETE CORRECTED VERSION
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class APIFetcherService {
    constructor() {
        this.tempDir = path.join(__dirname, '..', 'temp-api-data');
        this.ensureTempDirectory();
        console.log('API Fetcher Service initialized with complete empty response handling');
    }

    async ensureTempDirectory() {
        try {
            await fs.mkdir(this.tempDir, { recursive: true });
        } catch (error) {
            if (error.code !== 'EEXIST') {
                console.error('Failed to create temp directory:', error);
            }
        }
    }

    isValidJSONResponse(response) {
        try {
            console.log(`=== ENHANCED RESPONSE VALIDATION ===`);
            console.log(`Status: ${response.status}`);
            console.log(`Content-Type: ${response.headers['content-type'] || 'unknown'}`);

            // Check Content-Type header first
            const contentType = response.headers['content-type'] || '';

            // If Content-Type indicates HTML, it's definitely not JSON
            if (contentType.toLowerCase().includes('text/html')) {
                console.log('❌ Content-Type indicates HTML response');
                return {
                    isValid: false,
                    reason: 'HTML_CONTENT_TYPE',
                    details: `API returned Content-Type: ${contentType} (expected application/json)`
                };
            }

            // Handle object responses (already parsed by axios)
            if (typeof response.data === 'object' && response.data !== null) {
                console.log('✅ Response is already a valid JSON object');
                return { isValid: true };
            }

            // Handle string responses that need JSON parsing
            if (typeof response.data === 'string') {
                const responseStr = response.data;
                console.log(`String response length: ${responseStr.length} characters`);

                // Check for empty responses
                const trimmedResponse = responseStr.trim();
                if (trimmedResponse === '') {
                    console.log('❌ Empty string response');
                    return {
                        isValid: false,
                        reason: 'EMPTY_RESPONSE',
                        details: 'API returned empty string response'
                    };
                }

                if (trimmedResponse.length < 2) {
                    console.log('❌ Response too short');
                    return {
                        isValid: false,
                        reason: 'RESPONSE_TOO_SHORT',
                        details: `Response only ${trimmedResponse.length} characters: "${trimmedResponse}"`
                    };
                }

                // Check for HTML content in string
                if (trimmedResponse.startsWith('<!DOCTYPE') ||
                    trimmedResponse.startsWith('<html') ||
                    trimmedResponse.includes('<title>')) {
                    console.log('❌ String response contains HTML');
                    return {
                        isValid: false,
                        reason: 'HTML_CONTENT',
                        details: 'Response contains HTML markup instead of JSON'
                    };
                }

                // Enhanced JSON validation with truncation detection
                try {
                    console.log('Attempting to parse JSON string...');
                    console.log(`First 50 chars: "${trimmedResponse.substring(0, 50)}"`);
                    console.log(`Last 50 chars: "${trimmedResponse.slice(-50)}"`);

                    // Check if response looks like it should be JSON
                    const looksLikeJSON = (trimmedResponse.startsWith('{') && trimmedResponse.endsWith('}')) ||
                                        (trimmedResponse.startsWith('[') && trimmedResponse.endsWith(']'));

                    if (!looksLikeJSON) {
                        console.log('❌ Response does not look like JSON (missing proper start/end brackets)');
                        return {
                            isValid: false,
                            reason: 'NOT_JSON_FORMAT',
                            details: `Response does not start/end with JSON brackets. Starts with: "${trimmedResponse.substring(0, 10)}", Ends with: "${trimmedResponse.slice(-10)}"`
                        };
                    }

                    const parsed = JSON.parse(trimmedResponse);
                    console.log('✅ JSON parsing successful');

                    // Store parsed data for later use
                    response.data = parsed;

                    return { isValid: true, parsedData: parsed };

                } catch (parseError) {
                    console.log(`❌ JSON parsing failed: ${parseError.message}`);

                    // Provide specific diagnosis based on error type
                    if (parseError.message.includes('Unexpected end of JSON input')) {
                        // Check if response looks truncated
                        const lastChar = trimmedResponse.slice(-1);
                        const startsWithBracket = trimmedResponse.startsWith('{') || trimmedResponse.startsWith('[');
                        const endsWithBracket = lastChar === '}' || lastChar === ']';

                        if (startsWithBracket && !endsWithBracket) {
                            return {
                                isValid: false,
                                reason: 'TRUNCATED_JSON',
                                details: `JSON response appears truncated. Starts correctly but doesn't end properly. Last character: "${lastChar}". Response length: ${trimmedResponse.length} chars.`
                            };
                        } else {
                            return {
                                isValid: false,
                                reason: 'INCOMPLETE_JSON',
                                details: `JSON response is incomplete or corrupted. Response length: ${trimmedResponse.length} chars.`
                            };
                        }
                    } else if (parseError.message.includes('Unexpected token')) {
                        const match = parseError.message.match(/Unexpected token (.+) in JSON at position (\d+)/);
                        if (match) {
                            const position = parseInt(match[2]);
                            const contextStart = Math.max(0, position - 20);
                            const contextEnd = Math.min(trimmedResponse.length, position + 20);
                            const context = trimmedResponse.substring(contextStart, contextEnd);

                            return {
                                isValid: false,
                                reason: 'JSON_SYNTAX_ERROR',
                                details: `JSON syntax error at position ${position}. Context: "${context}"`
                            };
                        } else {
                            return {
                                isValid: false,
                                reason: 'JSON_SYNTAX_ERROR',
                                details: `JSON syntax error: ${parseError.message}`
                            };
                        }
                    } else {
                        return {
                            isValid: false,
                            reason: 'JSON_PARSE_ERROR',
                            details: `JSON parsing failed: ${parseError.message}`
                        };
                    }
                }
            }

            return { isValid: true };

        } catch (error) {
            console.error('Response validation failed:', error.message);
            return {
                isValid: false,
                reason: 'VALIDATION_ERROR',
                details: error.message
            };
        }
    }

    async testAPIConnection(config) {
        try {
            console.log('Testing API connection with enhanced validation...');
            console.log(`URL: ${config.url}`);
            console.log(`Auth: ${this.getAuthType(config)}`);

            const testConfig = {
                method: 'HEAD',
                url: config.url,
                timeout: 30000,
                maxRedirects: 5,
                validateStatus: function (status) {
                    return status < 500;
                },
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'ETL-Validation-Dashboard/1.0'
                }
            };

            this.configureAuthentication(testConfig, config);

            const startTime = Date.now();

            let response;
            try {
                response = await axios(testConfig);
            } catch (headError) {
                console.log('HEAD request failed, trying GET with limit...');
                testConfig.method = 'GET';

                // Add query parameters to limit response size
                if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                    testConfig.params = { sysparm_limit: 1 };
                } else {
                    testConfig.params = { limit: 1, size: 1, count: 1 };
                }

                response = await axios(testConfig);
            }

            const duration = Date.now() - startTime;

            // SPECIAL: Handle empty responses in connection test
            if (typeof response.data === 'string' && response.data.trim() === '') {
                console.log('📋 Empty response in connection test - trying alternative tables...');

                if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                    // Try incident table which usually has data
                    const alternativeUrl = config.url.replace(/\/table\/[^/?]+/, '/table/incident');
                    console.log(`🔄 Testing alternative ServiceNow table: ${alternativeUrl}`);

                    try {
                        const altTestConfig = { ...testConfig };
                        altTestConfig.url = alternativeUrl;
                        altTestConfig.params = { sysparm_limit: 1 };

                        const altResponse = await axios(altTestConfig);

                        if (altResponse.data && typeof altResponse.data === 'object') {
                            console.log('✅ Alternative table has data - original table appears empty');
                            return {
                                success: true,
                                connectionSuccessful: true,
                                authenticationSuccessful: true,
                                status: response.status,
                                statusText: response.statusText,
                                duration: duration,
                                authType: this.getAuthType(config),
                                contentType: response.headers['content-type'] || 'unknown',
                                message: 'Connection successful - but original table appears empty',
                                authMessage: 'Authentication successful',
                                warning: 'Original table appears empty',
                                suggestions: [
                                    'Your authentication and connection are working perfectly',
                                    'The "domain" table appears to be empty or inaccessible',
                                    'Try these ServiceNow tables with data: incident, sys_user, cmdb_ci',
                                    'Or add query parameters to your current URL: ?sysparm_limit=10'
                                ]
                            };
                        }
                    } catch (altError) {
                        console.log('Alternative table test failed:', altError.message);
                    }
                }

                // Return success but with warning about empty table
                return {
                    success: true,
                    connectionSuccessful: true,
                    authenticationSuccessful: true,
                    status: response.status,
                    statusText: response.statusText,
                    duration: duration,
                    authType: this.getAuthType(config),
                    contentType: response.headers['content-type'] || 'unknown',
                    message: 'Connection successful - table appears empty',
                    authMessage: 'Authentication successful',
                    warning: 'Table appears to be empty',
                    suggestions: [
                        'Connection and authentication are working correctly',
                        'The table might be empty or require additional permissions',
                        'Try a different table name or add query parameters'
                    ]
                };
            }

            // Enhanced response validation
            const validationResult = this.isValidJSONResponse(response);

            if (!validationResult.isValid) {
                console.log(`❌ Invalid response detected: ${validationResult.reason}`);

                return {
                    success: false,
                    connectionSuccessful: false,
                    authenticationSuccessful: response.status !== 401 && response.status !== 403,
                    error: validationResult.details,
                    details: validationResult.reason,
                    httpStatus: response.status,
                    contentType: response.headers['content-type'] || 'unknown',
                    authType: this.getAuthType(config),
                    duration: duration,
                    suggestions: this.getResponseErrorSuggestions(validationResult.reason, response.status)
                };
            }

            const connectionSuccessful = response.status >= 200 && response.status < 300;
            const authenticationSuccessful = response.status !== 401 && response.status !== 403;

            return {
                success: true,
                connectionSuccessful: connectionSuccessful,
                authenticationSuccessful: authenticationSuccessful,
                status: response.status,
                statusText: response.statusText,
                duration: duration,
                authType: this.getAuthType(config),
                contentType: response.headers['content-type'] || 'unknown',
                message: connectionSuccessful ? 'Connection successful' : `API returned ${response.status}`,
                authMessage: authenticationSuccessful ? 'Authentication successful' : 'Authentication failed'
            };

        } catch (error) {
            console.error('Connection test failed:', error.message);

            let errorMessage = 'Connection test failed';
            let authenticationFailed = false;

            if (error.response?.status === 401) {
                errorMessage = 'Authentication Failed (401 Unauthorized)';
                authenticationFailed = true;
            } else if (error.response?.status === 403) {
                errorMessage = 'Access Forbidden (403 Forbidden)';
                authenticationFailed = true;
            } else if (error.code === 'ECONNREFUSED') {
                errorMessage = 'Connection Refused - API server not reachable';
            } else if (error.code === 'ETIMEDOUT') {
                errorMessage = 'Connection Timeout - API server too slow';
            }

            return {
                success: false,
                connectionSuccessful: false,
                authenticationSuccessful: !authenticationFailed,
                error: errorMessage,
                details: error.message,
                httpStatus: error.response?.status || 'NO_RESPONSE',
                authType: this.getAuthType(config)
            };
        }
    }

    async fetchAPIData(config) {
        try {
            console.log('=== ENHANCED API DATA FETCH WITH EMPTY TABLE HANDLING ===');
            console.log(`URL: ${config.url}`);
            console.log(`Method: ${config.method || 'GET'}`);
            console.log(`Auth: ${this.getAuthType(config)}`);

            const requestConfig = {
                method: config.method || 'GET',
                url: config.url,
                timeout: 60000,
                maxRedirects: 5,
                validateStatus: function (status) {
                    return status < 500;
                },
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'ETL-Validation-Dashboard/1.0'
                }
            };

            this.configureAuthentication(requestConfig, config);

            // Add request body for POST/PUT methods
            if (['POST', 'PUT', 'PATCH'].includes((config.method || 'GET').toUpperCase())) {
                if (config.body) {
                    try {
                        requestConfig.data = JSON.parse(config.body);
                        requestConfig.headers['Content-Type'] = 'application/json';
                    } catch (bodyParseError) {
                        console.warn('Request body is not valid JSON, sending as string');
                        requestConfig.data = config.body;
                    }
                }
            }

            // Enhanced ServiceNow API handling
            if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                requestConfig.headers['Accept'] = 'application/json';
                requestConfig.headers['Content-Type'] = 'application/json';

                if (!requestConfig.params) {
                    requestConfig.params = {};
                }

                // Set reasonable defaults for ServiceNow
                if (!requestConfig.params.sysparm_limit) {
                    requestConfig.params.sysparm_limit = 20;
                }

                console.log('🔧 ServiceNow parameters:', requestConfig.params);
            }

            console.log('Making API request...');
            const startTime = Date.now();
            const response = await axios(requestConfig);
            const duration = Date.now() - startTime;

            console.log(`✅ API request completed in ${duration}ms`);
            console.log(`Status: ${response.status} ${response.statusText}`);
            console.log(`Content-Type: ${response.headers['content-type'] || 'unknown'}`);
            console.log(`Response type: ${typeof response.data}`);

            // Log response details for debugging
            if (typeof response.data === 'string') {
                console.log(`String response length: ${response.data.length} characters`);
                if (response.data.length === 0) {
                    console.log('🔍 EMPTY STRING RESPONSE DETECTED');
                }
            } else if (typeof response.data === 'object') {
                console.log(`Object response keys: [${Object.keys(response.data).join(', ')}]`);
                if (response.data.result && Array.isArray(response.data.result)) {
                    console.log(`ServiceNow result array length: ${response.data.result.length}`);
                }
            }

            // ENHANCED: Handle empty responses intelligently
            if (typeof response.data === 'string' && response.data.trim() === '') {
                console.log('📋 Empty string response - trying ServiceNow tables with data...');

                if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                    const tablesToTry = ['incident', 'sys_user', 'cmdb_ci', 'sys_db_object'];
                    let foundDataTable = null;

                    for (const tableName of tablesToTry) {
                        try {
                            const testUrl = config.url.replace(/\/table\/[^/?]+/, `/table/${tableName}`);
                            console.log(`🔄 Testing table: ${tableName}`);

                            const testConfig = { ...requestConfig };
                            testConfig.url = testUrl;
                            testConfig.params = { sysparm_limit: 1 };

                            const testResponse = await axios(testConfig);

                            if (testResponse.data &&
                                typeof testResponse.data === 'object' &&
                                testResponse.data.result &&
                                Array.isArray(testResponse.data.result) &&
                                testResponse.data.result.length > 0) {

                                console.log(`✅ Found data in ${tableName} table!`);
                                foundDataTable = tableName;
                                break;
                            }
                        } catch (testError) {
                            console.log(`Table ${tableName} test failed:`, testError.message);
                        }
                    }

                    if (foundDataTable) {
                        return {
                            success: false,
                            error: 'ServiceNow Table Empty',
                            details: `The "${config.url.split('/table/')[1]?.split('?')[0] || 'unknown'}" table appears to be empty, but "${foundDataTable}" table has data`,
                            httpStatus: response.status,
                            emptyTable: true,
                            alternativeTable: foundDataTable,
                            suggestions: [
                                `✅ Your authentication and connection are working perfectly!`,
                                `❌ The current table appears to be empty`,
                                `💡 Try using: ${config.url.replace(/\/table\/[^/?]+/, `/table/${foundDataTable}`)}`,
                                `🔍 Alternative tables with data: ${tablesToTry.join(', ')}`,
                                'Or modify your current URL to add filters: ?sysparm_query=active=true'
                            ]
                        };
                    } else {
                        return {
                            success: false,
                            error: 'ServiceNow Tables Empty or Access Restricted',
                            details: 'Cannot find any ServiceNow tables with accessible data',
                            httpStatus: response.status,
                            emptyTable: true,
                            suggestions: [
                                '✅ Your authentication is working correctly',
                                '❌ The tables appear empty or access is restricted',
                                'Contact your ServiceNow administrator to verify table access permissions',
                                'Check if there are specific roles required for API access',
                                'Verify the instance has data in standard tables'
                            ]
                        };
                    }
                }
            }

            // Handle object responses that might be empty
            if (typeof response.data === 'object' && response.data !== null) {
                // ServiceNow specific empty response handling
                if (response.data.result && Array.isArray(response.data.result) && response.data.result.length === 0) {
                    console.log('📋 ServiceNow returned empty result array');

                    return {
                        success: false,
                        error: 'ServiceNow Table Empty',
                        details: 'API is working but the table contains no records',
                        httpStatus: response.status,
                        emptyTable: true,
                        responseData: response.data,
                        suggestions: [
                            '✅ API connection and authentication successful',
                            '❌ The table contains no records',
                            'Try a different ServiceNow table: incident, sys_user, cmdb_ci',
                            'Add query parameters: ?sysparm_query=active=true&sysparm_limit=10',
                            'Check ServiceNow console to verify table has data'
                        ]
                    };
                }
            }

            // CRITICAL: Enhanced response validation
            const validationResult = this.isValidJSONResponse(response);

            if (!validationResult.isValid) {
                console.error(`❌ Invalid API response: ${validationResult.reason}`);

                return {
                    success: false,
                    error: 'Invalid API Response',
                    details: validationResult.details,
                    reason: validationResult.reason,
                    httpStatus: response.status,
                    statusText: response.statusText,
                    contentType: response.headers['content-type'] || 'unknown',
                    responseLength: typeof response.data === 'string' ? response.data.length : 'object',
                    authenticationRequired: response.status === 401,
                    accessDenied: response.status === 403,
                    responsePreview: this.getResponsePreview(response.data),
                    suggestions: this.getResponseErrorSuggestions(validationResult.reason, response.status)
                };
            }

            // Check for authentication errors
            if (response.status === 401) {
                return {
                    success: false,
                    error: 'Authentication Required',
                    details: 'API returned 401 Unauthorized',
                    httpStatus: 401,
                    authenticationRequired: true,
                    suggestions: [
                        'Configure Basic Authentication with valid username/password',
                        'For ServiceNow: use your ServiceNow login credentials'
                    ]
                };
            }

            if (response.status === 403) {
                return {
                    success: false,
                    error: 'Access Forbidden',
                    details: 'API returned 403 Forbidden - valid credentials but insufficient permissions',
                    httpStatus: 403,
                    accessDenied: true,
                    suggestions: [
                        'Contact your administrator for proper API access rights'
                    ]
                };
            }

            // Additional check for error responses
            if (this.isErrorResponse(response.data, response.status)) {
                console.log('❌ API returned valid JSON but with error content');

                return {
                    success: false,
                    error: 'API Error Response',
                    details: this.extractErrorDetails(response.data),
                    httpStatus: response.status,
                    responseData: response.data,
                    suggestions: [
                        'API returned an error instead of data',
                        'Check the API endpoint URL',
                        'Verify authentication credentials'
                    ]
                };
            }

            // SUCCESS: Process valid JSON response
            console.log('✅ Valid JSON response detected, processing...');

            const dataId = uuidv4();

            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: response.status,
                statusText: response.statusText,
                contentType: response.headers['content-type'] || 'unknown',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(response.data).length,
                duration: duration,
                dataType: this.detectDataType(response.data),
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true
            };

            // Save API data
            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
            await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

            console.log(`✅ API data saved successfully: ${dataFilePath}`);

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: this.createPreview(response.data),
                message: `API data fetched successfully - ${this.formatDataSize(JSON.stringify(response.data).length)}`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success'
            };

        } catch (error) {
            console.error('=== API FETCH FAILED ===');
            console.error(`Error: ${error.message}`);

            let errorMessage = 'API request failed';
            let suggestions = [
                'Check the API endpoint URL',
                'Verify your internet connection',
                'Try testing the connection first'
            ];

            if (error.response?.status === 401) {
                errorMessage = 'Authentication Required (401 Unauthorized)';
                suggestions = [
                    'Configure authentication credentials',
                    'For ServiceNow: use Basic Authentication with username/password'
                ];
            } else if (error.response?.status === 403) {
                errorMessage = 'Access Forbidden (403 Forbidden)';
                suggestions = [
                    'Your credentials are valid but you lack permissions',
                    'Contact your administrator for proper API access rights'
                ];
            } else if (error.code === 'ECONNREFUSED') {
                errorMessage = 'Connection Refused - Cannot reach API server';
                suggestions = [
                    'Check if the API server is running',
                    'Verify the URL is correct'
                ];
            } else if (error.code === 'ETIMEDOUT') {
                errorMessage = 'Request Timeout - API response too slow';
                suggestions = [
                    'API server took too long to respond',
                    'Try reducing response size with query parameters'
                ];
            }

            return {
                success: false,
                error: errorMessage,
                details: error.message,
                connectionStatus: 'FAILED',
                httpStatus: error.response?.status || 'NO_RESPONSE',
                suggestions: suggestions
            };
        }
    }

    // Helper method to get response preview for debugging
    getResponsePreview(data) {
        try {
            if (typeof data === 'string') {
                return data.substring(0, 200) + (data.length > 200 ? '...' : '');
            } else {
                return JSON.stringify(data, null, 2).substring(0, 200) + '...';
            }
        } catch (error) {
            return 'Could not generate preview';
        }
    }

    getResponseErrorSuggestions(reason, httpStatus) {
        const suggestions = [];

        switch (reason) {
            case 'EMPTY_RESPONSE':
                suggestions.push('✅ Authentication is working - table appears empty');
                suggestions.push('Try ServiceNow tables with data: incident, sys_user, cmdb_ci');
                suggestions.push('Add query parameters: ?sysparm_limit=10&sysparm_query=active=true');
                suggestions.push('Check ServiceNow console to verify table has records');
                break;

            case 'RESPONSE_TOO_SHORT':
                suggestions.push('API returned very short response');
                suggestions.push('Check if the endpoint is correct');
                suggestions.push('For ServiceNow, ensure URL format: /api/now/table/[table_name]');
                break;

            case 'TRUNCATED_JSON':
            case 'INCOMPLETE_JSON':
                suggestions.push('API response was cut off or truncated');
                suggestions.push('Reduce response size: add sysparm_limit=20');
                suggestions.push('Add field selection: sysparm_fields=sys_id,name,active');
                break;

            case 'JSON_SYNTAX_ERROR':
            case 'JSON_PARSE_ERROR':
                suggestions.push('API returned invalid JSON syntax');
                suggestions.push('Check API documentation for correct endpoint format');
                break;

            case 'NOT_JSON_FORMAT':
                suggestions.push('Response does not appear to be JSON format');
                suggestions.push('For ServiceNow: ensure URL ends with /api/now/table/[table_name]');
                break;

            case 'HTML_CONTENT_TYPE':
            case 'HTML_CONTENT':
                suggestions.push('API is returning HTML instead of JSON');
                suggestions.push('Check if the API URL is correct');
                suggestions.push('Verify authentication - server might be redirecting');
                break;

            default:
                suggestions.push('Check API endpoint URL and authentication');
        }

        return suggestions;
    }

    async getAPIData(dataId) {
        try {
            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            const [dataContent, metadataContent] = await Promise.all([
                fs.readFile(dataFilePath, 'utf8'),
                fs.readFile(metadataFilePath, 'utf8')
            ]);

            return {
                success: true,
                data: JSON.parse(dataContent),
                metadata: JSON.parse(metadataContent)
            };

        } catch (error) {
            return {
                success: false,
                error: 'API data not found or expired',
                details: error.message
            };
        }
    }

    async getAPIDataPreview(dataId) {
        try {
            const result = await this.getAPIData(dataId);
            if (!result.success) return result;

            const preview = this.createPreview(result.data);

            return {
                success: true,
                preview: {
                    ...preview,
                    metadata: result.metadata,
                    dataId: dataId
                }
            };

        } catch (error) {
            return {
                success: false,
                error: 'Failed to create preview',
                details: error.message
            };
        }
    }

    async cleanupAPIData(dataId) {
        try {
            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            await Promise.all([
                fs.unlink(dataFilePath).catch(() => {}),
                fs.unlink(metadataFilePath).catch(() => {})
            ]);

            console.log(`API data cleanup completed: ${dataId}`);
            return { success: true };

        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    getAuthType(config) {
        if (config.username && config.password) {
            return 'Basic Authentication';
        } else if (config.headers) {
            const headerStr = typeof config.headers === 'string' ? config.headers : JSON.stringify(config.headers);
            if (headerStr.toLowerCase().includes('x-api-key')) {
                return 'API Key Authentication';
            } else if (headerStr.toLowerCase().includes('bearer')) {
                return 'Bearer Token Authentication';
            } else {
                return 'Custom Headers';
            }
        } else {
            return 'No Authentication';
        }
    }

    configureAuthentication(requestConfig, config) {
        // Basic Authentication
        if (config.username && config.password) {
            const credentials = Buffer.from(`${config.username}:${config.password}`).toString('base64');
            requestConfig.headers['Authorization'] = `Basic ${credentials}`;
            console.log('✅ Basic authentication configured');
        }

        // Custom Headers
        if (config.headers && Object.keys(config.headers).length > 0) {
            if (typeof config.headers === 'string') {
                const headerLines = config.headers.split('\n');
                for (const line of headerLines) {
                    const [key, ...valueParts] = line.split(':');
                    if (key && valueParts.length > 0) {
                        requestConfig.headers[key.trim()] = valueParts.join(':').trim();
                    }
                }
            } else {
                Object.assign(requestConfig.headers, config.headers);
            }
            console.log('✅ Custom headers configured');
        }
    }

    isErrorResponse(data, status) {
        if (status >= 400) {
            return true;
        }

        if (typeof data === 'object' && data !== null) {
            const dataStr = JSON.stringify(data).toLowerCase();

            const hasErrorFields = dataStr.includes('"error"') ||
                                 dataStr.includes('"errors"') ||
                                 dataStr.includes('"unauthorized') ||
                                 dataStr.includes('"forbidden');

            const hasSuccessFields = dataStr.includes('"result"') ||
                                   dataStr.includes('"data"') ||
                                   dataStr.includes('"sys_id');

            return hasErrorFields && !hasSuccessFields;
        }

        return false;
    }

    extractErrorDetails(data) {
        if (typeof data === 'object' && data !== null) {
            if (data.error) {
                return typeof data.error === 'string' ? data.error : JSON.stringify(data.error);
            }
            if (data.error_message) return data.error_message;
            if (data.message) return data.message;
            if (data.errors) {
                return Array.isArray(data.errors) ? data.errors.join(', ') : JSON.stringify(data.errors);
            }

            return `API returned error response: ${JSON.stringify(data, null, 2)}`;
        } else if (typeof data === 'string') {
            if (data.includes('<!DOCTYPE') || data.includes('<html')) {
                return 'API returned HTML page instead of JSON';
            }
            return data.substring(0, 500) + (data.length > 500 ? '...' : '');
        }

        return 'Unknown error response format';
    }

    detectDataType(data) {
        if (Array.isArray(data)) {
            return `Array (${data.length} items)`;
        } else if (typeof data === 'object' && data !== null) {
            if (data.result && Array.isArray(data.result)) {
                return `ServiceNow Response (${data.result.length} records)`;
            } else if (data.result) {
                return 'ServiceNow Response (single record)';
            } else {
                return `Object (${Object.keys(data).length} properties)`;
            }
        } else {
            return typeof data;
        }
    }

    createPreview(data) {
        try {
            let records = [];
            let totalRecords = 0;
            let format = 'Unknown';

            // Handle ServiceNow API response format
            if (typeof data === 'object' && data !== null && data.result) {
                if (Array.isArray(data.result)) {
                    records = data.result;
                    totalRecords = data.result.length;
                    format = 'ServiceNow API Response';
                } else {
                    records = [data.result];
                    totalRecords = 1;
                    format = 'ServiceNow API Response';
                }
            } else if (Array.isArray(data)) {
                records = data;
                totalRecords = data.length;
                format = 'JSON Array';
            } else if (typeof data === 'object' && data !== null) {
                records = [data];
                totalRecords = 1;
                format = 'JSON Object';
            } else {
                return {
                    totalRecords: 0,
                    fieldsDetected: 0,
                    format: 'Unsupported',
                    error: 'API response is not valid JSON'
                };
            }

            if (!records[0]) {
                return {
                    totalRecords: totalRecords,
                    fieldsDetected: 0,
                    format: format,
                    error: 'No records found in API response'
                };
            }

            const firstRecord = records[0];
            const flattenedSample = this.flattenObject(firstRecord);
            const allFields = Object.keys(flattenedSample);

            const idFields = allFields.filter(field => {
                const lowerField = field.toLowerCase();
                return lowerField.includes('id') ||
                       lowerField.includes('key') ||
                       lowerField.includes('number') ||
                       lowerField === 'sys_id';
            });

            const importantFields = allFields.filter(field => {
                const lowerField = field.toLowerCase();
                return !idFields.includes(field) && (
                    lowerField.includes('name') ||
                    lowerField.includes('status') ||
                    lowerField.includes('type') ||
                    lowerField.includes('active') ||
                    lowerField.includes('description')
                );
            });

            return {
                totalRecords: totalRecords,
                fieldsDetected: allFields.length,
                fileSize: JSON.stringify(data).length,
                format: format,
                sampleRecords: [flattenedSample],
                availableFields: allFields,
                idFields: idFields,
                importantFields: importantFields,
                originalSample: firstRecord
            };

        } catch (error) {
            console.error('Preview creation failed:', error.message);
            return {
                totalRecords: 0,
                fieldsDetected: 0,
                format: 'Error',
                error: `Preview generation failed: ${error.message}`
            };
        }
    }

    flattenObject(obj, prefix = '', depth = 0) {
        const flattened = {};

        if (depth > 5) return flattened;

        for (const [key, value] of Object.entries(obj)) {
            const cleanKey = prefix ? `${prefix}_${key}` : key;

            if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                if (value.display_value !== undefined || value.link !== undefined || value.value !== undefined) {
                    if (value.display_value !== undefined) {
                        flattened[`${cleanKey}_display_value`] = value.display_value;
                    }
                    if (value.link !== undefined) {
                        flattened[`${cleanKey}_link`] = value.link;
                    }
                    if (value.value !== undefined) {
                        flattened[`${cleanKey}_value`] = value.value;
                    }
                } else {
                    Object.assign(flattened, this.flattenObject(value, cleanKey, depth + 1));
                }
            } else if (Array.isArray(value)) {
                flattened[cleanKey] = JSON.stringify(value);
            } else {
                flattened[cleanKey] = value;
            }
        }

        return flattened;
    }

    formatDataSize(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
}

module.exports = APIFetcherService;