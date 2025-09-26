// services/api-fetcher.js - COMPLETE WITH NEW ALL RECORDS + FIRST PAGE COMPARISON
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class APIFetcherService {
    constructor() {
        this.tempDir = path.join(__dirname, '..', 'temp-api-data');
        this.ensureTempDirectory();
        console.log('API Fetcher Service initialized with complete empty response handling, pagination support, and ALL RECORDS + FIRST PAGE COMPARISON');
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

                // Dynamically add appropriate limit parameters based on URL patterns
                testConfig.params = this.getGenericLimitParams(config.url);

                response = await axios(testConfig);
            }

            const duration = Date.now() - startTime;

            // SPECIAL: Handle empty responses in connection test
            if (typeof response.data === 'string' && response.data.trim() === '') {
                console.log('📋 Empty response in connection test - trying alternative tables...');

                if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                    // Try incident table which usually has data
                    const alternativeUrl = config.url.replace(/\/table\/[^/?]+/, '/table/incident');
                    console.log(`📄 Testing alternative ServiceNow table: ${alternativeUrl}`);

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

    // NEW: Fetch all records but return first page for comparison + total count
        // NEW: Fetch first page specifically for comparison
    async fetchFirstPageForComparison(config) {
        try {
            console.log('Fetching first page for comparison...');

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

            // Add first page parameters to ensure we get a reasonable sample
            const firstPageUrl = new URL(config.url);

            // Set pagination parameters for first page with reasonable size
            if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                firstPageUrl.searchParams.set('sysparm_limit', '100');
                firstPageUrl.searchParams.set('sysparm_offset', '0');
            } else if (config.url.includes('github.com/api') || config.url.includes('api.github.com')) {
                firstPageUrl.searchParams.set('per_page', '100');
                firstPageUrl.searchParams.set('page', '1');
            } else {
                // Generic pagination
                if (!firstPageUrl.searchParams.has('limit') && !firstPageUrl.searchParams.has('per_page')) {
                    firstPageUrl.searchParams.set('limit', '100');
                    firstPageUrl.searchParams.set('page', '1');
                }
            }

            requestConfig.url = firstPageUrl.toString();

            console.log(`First page URL: ${requestConfig.url}`);

            const response = await axios(requestConfig);

            // Validate response
            const validationResult = this.isValidJSONResponse(response);
            if (!validationResult.isValid) {
                throw new Error(`Invalid first page response: ${validationResult.details}`);
            }

            // Extract records from response
            let records = [];
            if (response.data.result && Array.isArray(response.data.result)) {
                records = response.data.result;
            } else if (Array.isArray(response.data)) {
                records = response.data;
            } else if (response.data.data && Array.isArray(response.data.data)) {
                records = response.data.data;
            } else if (typeof response.data === 'object' && response.data !== null) {
                records = [response.data];
            }

            console.log(`First page extracted: ${records.length} records`);

            return {
                success: true,
                data: response.data,
                records: records,
                status: response.status,
                headers: response.headers
            };

        } catch (error) {
            console.error('First page fetch failed:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    // NEW: Format first page data with total count metadata
    formatFirstPageWithTotalCount(firstPageRecords, originalResponse, totalCount) {
        // Preserve original API structure but add total count metadata
        if (originalResponse && originalResponse.result !== undefined) {
            return {
                ...originalResponse,
                result: firstPageRecords,
                result_info: {
                    ...originalResponse.result_info,
                    total_count: totalCount,
                    records_for_comparison: firstPageRecords.length,
                    comparison_strategy: 'first-page-with-total-count'
                }
            };
        } else if (originalResponse && originalResponse.incidents !== undefined) {
            return {
                ...originalResponse,
                incidents: firstPageRecords,
                total: totalCount,
                records_for_comparison: firstPageRecords.length,
                comparison_strategy: 'first-page-with-total-count'
            };
        } else {
            return {
                data: firstPageRecords,
                total_count: totalCount,
                records_for_comparison: firstPageRecords.length,
                comparison_strategy: 'first-page-with-total-count'
            };
        }
    }

    /// SYNTAX ERROR FIX for api-fetcher.js
// The error occurs because 'useAllRecordsStrategy' is declared twice in your file

// OPTION 1: Find this existing line in your file and REMOVE it:
// const useAllRecordsStrategy = config.fetchAllWithFirstPageComparison || config._useAllRecordsStrategy;

// OPTION 2: Or modify the existing line to use 'let' instead of 'const':
// let useAllRecordsStrategy = config.fetchAllWithFirstPageComparison || config._useAllRecordsStrategy;

// OPTION 3: Complete replacement - replace your ENTIRE existing fetchAPIData function with this:

async fetchAPIData(config) {
    try {
        // PRIORITY CHECK FIRST - Detect if user specified pagination parameters
        const url = new URL(config.url);
        const hasUserPaginationParams = url.searchParams.has('per_page') ||
                                       url.searchParams.has('limit') ||
                                       url.searchParams.has('page_size') ||
                                       url.searchParams.has('sysparm_limit') ||
                                       url.searchParams.has('page') ||
                                       url.searchParams.has('offset');

        if (hasUserPaginationParams) {
            // USER PAGINATION DETECTED - Skip all other strategies
            console.log('=== USER PAGINATION DETECTED - DIRECT REQUEST STRATEGY ===');
            console.log(`URL: ${config.url}`);
            console.log(`Method: ${config.method || 'GET'}`);
            console.log(`Auth: ${this.getAuthType(config)}`);
            console.log('User pagination params:', Object.fromEntries(url.searchParams.entries()));
            console.log('Making direct request with user-specified parameters - no sampling or modification');

            return await this.handleDirectUserRequest(config, url);
        }

        // NO USER PAGINATION - Proceed with smart strategies
        console.log('=== SMART API DATA FETCH WITH SAMPLE + COUNT APPROACH ===');
        console.log(`URL: ${config.url}`);
        console.log(`Method: ${config.method || 'GET'}`);
        console.log(`Auth: ${this.getAuthType(config)}`);

        // Check for all records strategy request (SINGLE DECLARATION)

        // DEFAULT: Smart sampling strategy
        console.log('SMART SAMPLING STRATEGY: Efficient sample with total count');

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

        // Configure authentication
        this.configureAuthentication(requestConfig, config);

        // Add request body for POST/PUT/PATCH
        if (['POST', 'PUT', 'PATCH'].includes((config.method || 'GET').toUpperCase())) {
            if (config.body) {
                try {
                    requestConfig.data = JSON.parse(config.body);
                    requestConfig.headers['Content-Type'] = 'application/json';
                } catch (bodyParseError) {
                    requestConfig.data = config.body;
                }
            }
        }

        // Add custom headers
        if (config.headers && typeof config.headers === 'object') {
            Object.assign(requestConfig.headers, config.headers);
        }

        console.log('Making smart sampling API request...');
        const startTime = Date.now();
        const response = await axios(requestConfig);
        const duration = Date.now() - startTime;

        console.log(`Smart sampling request completed: ${response.status} in ${duration}ms`);

        // Validate response
        const validationResult = this.isValidJSONResponse(response);
        if (!validationResult.isValid) {
            throw new Error(`Invalid response: ${validationResult.details}`);
        }

        // Extract data and count records
        let responseData = response.data;
        let recordCount = 0;
        let totalAvailable = 0;

        if (Array.isArray(responseData)) {
            recordCount = responseData.length;
            totalAvailable = recordCount; // Assume this is all available for simple arrays
        } else if (responseData.result && Array.isArray(responseData.result)) {
            recordCount = responseData.result.length;
            // Try to get total count from ServiceNow-style headers
            totalAvailable = this.extractTotalCount(response) || recordCount;
        } else if (responseData.data && Array.isArray(responseData.data)) {
            recordCount = responseData.data.length;
            totalAvailable = responseData.total || responseData.count || recordCount;
        } else {
            recordCount = 1;
            totalAvailable = 1;
        }

        console.log(`SMART SAMPLING SUCCESS: Got ${recordCount} records (${totalAvailable} total available)`);

        // Create metadata
        const dataId = this.generateUUID();
        const metadata = {
            id: dataId,
            url: config.url,
            method: config.method || 'GET',
            status: response.status,
            statusText: response.statusText,
            contentType: response.headers['content-type'] || 'unknown',
            fetchedAt: new Date().toISOString(),
            responseSize: JSON.stringify(responseData).length,
            duration: duration,
            dataType: this.detectDataType(responseData),
            authenticationUsed: this.getAuthType(config),
            authenticationStatus: 'success',
            connectionSuccessful: true,
            totalRecords: recordCount,
            totalRecordsAvailable: totalAvailable,
            totalRecordsInAPI: totalAvailable,
            recordsForComparison: recordCount,
            fetchStrategy: 'smart-sample-with-count',
            userPaginationRespected: false
        };

        // Save data and metadata
        const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
        const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

        await fs.writeFile(dataFilePath, JSON.stringify(responseData, null, 2));
        await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

        // Create enhanced preview
        const dataPreview = this.createEnhancedPreview(responseData, {
            totalRecordsAvailable: totalAvailable,
            sampleSize: recordCount,
            strategy: 'smart-sampling'
        });

        return {
            success: true,
            dataId: dataId,
            metadata: metadata,
            dataPreview: dataPreview,
            message: `Smart sampling successful - ${recordCount} records retrieved from ${totalAvailable} available`,
            connectionStatus: 'SUCCESS',
            authenticationStatus: 'success',
            userPaginationRespected: false
        };

    } catch (error) {
        console.error('fetchAPIData error:', error);

        // Enhanced error handling with specific cases
        if (error.code === 'ECONNREFUSED') {
            return {
                success: false,
                error: 'Connection refused - API endpoint unreachable',
                connectionStatus: 'FAILED',
                authenticationStatus: 'unknown'
            };
        }

        if (error.response) {
            const status = error.response.status;
            let authenticationStatus = 'unknown';
            let errorMessage = `HTTP ${status}: ${error.response.statusText || 'Request failed'}`;

            if (status === 401) {
                authenticationStatus = 'failed';
                errorMessage = 'Authentication failed - Invalid credentials';
            } else if (status === 403) {
                authenticationStatus = 'insufficient';
                errorMessage = 'Access forbidden - Insufficient permissions';
            } else if (status >= 400 && status < 500) {
                authenticationStatus = 'client_error';
                errorMessage = `Client error ${status}: ${error.response.statusText}`;
            } else if (status >= 500) {
                authenticationStatus = 'server_error';
                errorMessage = `Server error ${status}: ${error.response.statusText}`;
            }

            return {
                success: false,
                error: errorMessage,
                httpStatus: status,
                connectionStatus: 'FAILED',
                authenticationStatus: authenticationStatus,
                authenticationRequired: status === 401
            };
        }

        return {
            success: false,
            error: error.message || 'Unknown API fetch error',
            connectionStatus: 'FAILED',
            authenticationStatus: 'unknown'
        };
    }
}

// Add the separate handler method for direct user requests
async handleDirectUserRequest(config, url) {
    console.log('Processing direct user request without modification...');

    // Make direct request without any sampling or modification
    const requestConfig = {
        method: config.method || 'GET',
        url: config.url, // Use EXACT URL with user parameters
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

    if (['POST', 'PUT', 'PATCH'].includes((config.method || 'GET').toUpperCase())) {
        if (config.body) {
            try {
                requestConfig.data = JSON.parse(config.body);
                requestConfig.headers['Content-Type'] = 'application/json';
            } catch (bodyParseError) {
                requestConfig.data = config.body;
            }
        }
    }

    console.log('Making DIRECT API request with user pagination...');
    const startTime = Date.now();
    const response = await axios(requestConfig);
    const duration = Date.now() - startTime;

    console.log(`Direct request completed: ${response.status} in ${duration}ms`);

    // Validate response
    const validationResult = this.isValidJSONResponse(response);
    if (!validationResult.isValid) {
        throw new Error(`Invalid response: ${validationResult.details}`);
    }

    // Count records in response
    let recordCount = 0;
    let dataArray = [];

    if (Array.isArray(response.data)) {
        recordCount = response.data.length;
        dataArray = response.data;
    } else if (response.data.result && Array.isArray(response.data.result)) {
        recordCount = response.data.result.length;
        dataArray = response.data.result;
    } else if (response.data.data && Array.isArray(response.data.data)) {
        recordCount = response.data.data.length;
        dataArray = response.data.data;
    } else if (response.data.answers && Array.isArray(response.data.answers)) {
        recordCount = response.data.answers.length;
        dataArray = response.data.answers;
    } else {
        recordCount = 1;
        dataArray = [response.data];
    }

    // Try to determine if there are more records available (for informational purposes)
    let totalAvailableEstimate = recordCount;
    let isPartialResult = false;

    // Check for pagination indicators
    if (response.data.total) {
        totalAvailableEstimate = response.data.total;
        isPartialResult = recordCount < totalAvailableEstimate;
    } else if (response.data.count) {
        totalAvailableEstimate = response.data.count;
        isPartialResult = recordCount < totalAvailableEstimate;
    } else if (response.headers['x-total-count']) {
        totalAvailableEstimate = parseInt(response.headers['x-total-count']);
        isPartialResult = recordCount < totalAvailableEstimate;
    }

    // Extract user pagination parameters for metadata
    const userPaginationParams = Object.fromEntries(url.searchParams.entries());
    const paginationKeys = ['per_page', 'limit', 'page_size', 'sysparm_limit', 'page', 'offset'];
    const detectedPaginationParams = Object.keys(userPaginationParams).filter(key =>
        paginationKeys.includes(key)
    );

    console.log(`USER PAGINATION SUCCESS: Got ${recordCount} records as requested`);
    console.log(`Pagination parameters used: ${detectedPaginationParams.join(', ')}`);

    if (isPartialResult) {
        console.log(`Note: ${totalAvailableEstimate} total records available in API, user requested ${recordCount}`);
    }

    // Save and return result
    const dataId = this.generateUUID();
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
        connectionSuccessful: true,
        totalRecords: recordCount,
        totalRecordsInAPI: totalAvailableEstimate,
        totalRecordsAvailable: totalAvailableEstimate,
        recordsForComparison: recordCount,
        fetchStrategy: 'direct-user-request',
        userPaginationRespected: true,
        userPaginationParams: userPaginationParams,
        detectedPaginationParams: detectedPaginationParams,
        isPartialResult: isPartialResult,
        dataQualityNote: isPartialResult ?
            `User requested ${recordCount} records from ${totalAvailableEstimate} available` :
            'Complete dataset as requested by user'
    };

    const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
    const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

    await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
    await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

    // Create enhanced preview for direct user requests
    const dataPreview = this.createEnhancedPreview(response.data, {
        totalRecordsAvailable: totalAvailableEstimate,
        sampleSize: recordCount,
        strategy: 'direct-user-request',
        isUserRequested: true,
        paginationParams: detectedPaginationParams
    });

    return {
        success: true,
        dataId: dataId,
        metadata: metadata,
        dataPreview: dataPreview,
        message: isPartialResult ?
            `Direct request successful - ${recordCount} records retrieved (${totalAvailableEstimate} total available)` :
            `Direct request successful - ${recordCount} records retrieved as requested`,
        connectionStatus: 'SUCCESS',
        authenticationStatus: 'success',
        userPaginationRespected: true,
        totalRecordsInAPI: totalAvailableEstimate,
        recordsRetrieved: recordCount
    };
}

// Helper method to generate UUID (if you don't have one)
generateUUID() {
    return 'xxxx-xxxx-4xxx-yxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}// MODIFIED: fetchAPIData method to use the new strategy when appropriate
extractTotalCount(response) {
    try {
        // Try various ways to get total count from different API response formats

        // 1. Check response headers first
        if (response.headers['x-total-count']) {
            const headerCount = parseInt(response.headers['x-total-count']);
            if (!isNaN(headerCount)) {
                console.log(`Total count from header x-total-count: ${headerCount}`);
                return headerCount;
            }
        }

        // 2. Check other common header formats
        const totalHeaders = [
            'x-total-count',
            'x-total',
            'total-count',
            'total-records'
        ];

        for (const headerName of totalHeaders) {
            if (response.headers[headerName]) {
                const headerCount = parseInt(response.headers[headerName]);
                if (!isNaN(headerCount)) {
                    console.log(`Total count from header ${headerName}: ${headerCount}`);
                    return headerCount;
                }
            }
        }

        // 3. Check response data structure
        if (response.data && typeof response.data === 'object') {

            // ServiceNow style with result_info
            if (response.data.result_info) {
                const resultInfo = response.data.result_info;
                if (resultInfo.total_count !== undefined) {
                    console.log(`Total count from result_info.total_count: ${resultInfo.total_count}`);
                    return parseInt(resultInfo.total_count);
                }
                if (resultInfo.totalCount !== undefined) {
                    console.log(`Total count from result_info.totalCount: ${resultInfo.totalCount}`);
                    return parseInt(resultInfo.totalCount);
                }
                if (resultInfo.total !== undefined) {
                    console.log(`Total count from result_info.total: ${resultInfo.total}`);
                    return parseInt(resultInfo.total);
                }
            }

            // Direct total fields in response data
            if (response.data.total !== undefined) {
                console.log(`Total count from data.total: ${response.data.total}`);
                return parseInt(response.data.total);
            }

            if (response.data.count !== undefined) {
                console.log(`Total count from data.count: ${response.data.count}`);
                return parseInt(response.data.count);
            }

            if (response.data.totalCount !== undefined) {
                console.log(`Total count from data.totalCount: ${response.data.totalCount}`);
                return parseInt(response.data.totalCount);
            }

            if (response.data.total_count !== undefined) {
                console.log(`Total count from data.total_count: ${response.data.total_count}`);
                return parseInt(response.data.total_count);
            }

            // Pagination object
            if (response.data.pagination) {
                const p = response.data.pagination;
                if (p.total !== undefined) {
                    console.log(`Total count from pagination.total: ${p.total}`);
                    return parseInt(p.total);
                }
                if (p.count !== undefined) {
                    console.log(`Total count from pagination.count: ${p.count}`);
                    return parseInt(p.count);
                }
                if (p.totalRecords !== undefined) {
                    console.log(`Total count from pagination.totalRecords: ${p.totalRecords}`);
                    return parseInt(p.totalRecords);
                }
                if (p.total_count !== undefined) {
                    console.log(`Total count from pagination.total_count: ${p.total_count}`);
                    return parseInt(p.total_count);
                }
            }

            // Meta object (common in REST APIs)
            if (response.data.meta) {
                const m = response.data.meta;
                if (m.total !== undefined) {
                    console.log(`Total count from meta.total: ${m.total}`);
                    return parseInt(m.total);
                }
                if (m.count !== undefined) {
                    console.log(`Total count from meta.count: ${m.count}`);
                    return parseInt(m.count);
                }
                if (m.totalCount !== undefined) {
                    console.log(`Total count from meta.totalCount: ${m.totalCount}`);
                    return parseInt(m.totalCount);
                }
                if (m.total_records !== undefined) {
                    console.log(`Total count from meta.total_records: ${m.total_records}`);
                    return parseInt(m.total_records);
                }
            }

            // Check if data itself is an array (direct array response)
            if (Array.isArray(response.data)) {
                console.log(`Total count from array length: ${response.data.length}`);
                return response.data.length;
            }

            // Check nested data arrays
            if (response.data.result && Array.isArray(response.data.result)) {
                // This might be a partial result, but if no total info available, use length
                console.log(`Total count from result array length: ${response.data.result.length} (might be partial)`);
                return response.data.result.length;
            }

            if (response.data.data && Array.isArray(response.data.data)) {
                console.log(`Total count from data array length: ${response.data.data.length} (might be partial)`);
                return response.data.data.length;
            }

            if (response.data.items && Array.isArray(response.data.items)) {
                console.log(`Total count from items array length: ${response.data.items.length} (might be partial)`);
                return response.data.items.length;
            }

            if (response.data.records && Array.isArray(response.data.records)) {
                console.log(`Total count from records array length: ${response.data.records.length} (might be partial)`);
                return response.data.records.length;
            }
        }

        // 4. Last resort - check response status and assume 0 if empty
        console.log('No total count information found in response');
        return null;

    } catch (error) {
        console.error('Error extracting total count:', error.message);
        return null;
    }
}

// Also add this helper method if it's missing:

// NEW: Direct user request method - respects exactly what user specified
async fetchDirectUserRequest(config) {
    try {
        console.log('🎯 DIRECT USER REQUEST: Using EXACT user-specified URL and parameters');
        console.log(`EXACT URL: ${config.url}`);
        console.log('NO modifications, NO sampling, NO smart pagination');

        const requestConfig = {
            method: config.method || 'GET',
            url: config.url, // Use EXACT URL as provided by user
            timeout: 60000,
            maxRedirects: 5,
            validateStatus: function (status) {
                return status < 500;
            },
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'ETL-Validation-Dashboard/2.0'
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
                    requestConfig.data = config.body;
                }
            }
        }

        console.log('Making DIRECT API request with user parameters...');
        const startTime = Date.now();
        const response = await axios(requestConfig);
        const duration = Date.now() - startTime;

        console.log(`✅ DIRECT request completed in ${duration}ms`);
        console.log(`Status: ${response.status} ${response.statusText}`);

        // Log what we actually got
        if (typeof response.data === 'object' && response.data !== null) {
            let recordCount = 0;

            // Try to determine record count from response
            if (Array.isArray(response.data)) {
                recordCount = response.data.length;
                console.log(`📊 Direct array response: ${recordCount} records`);
            } else if (response.data.result && Array.isArray(response.data.result)) {
                recordCount = response.data.result.length;
                console.log(`📊 API response with result array: ${recordCount} records`);
            } else if (response.data.data && Array.isArray(response.data.data)) {
                recordCount = response.data.data.length;
                console.log(`📊 API response with data array: ${recordCount} records`);
            } else if (response.data.answers && Array.isArray(response.data.answers)) {
                recordCount = response.data.answers.length;
                console.log(`📊 Peakon-style response with answers array: ${recordCount} records`);
            } else {
                console.log(`📊 Single object response`);
                recordCount = 1;
            }

            console.log(`🎯 USER PAGINATION RESPECTED: Got ${recordCount} records as requested`);
        }

        // Validate response
        const validationResult = this.isValidJSONResponse(response);
        if (!validationResult.isValid) {
            throw new Error(`Invalid response: ${validationResult.details}`);
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
                    'Configure authentication for this API',
                    'Check your credentials and permissions'
                ]
            };
        }

        if (response.status === 403) {
            return {
                success: false,
                error: 'Access Forbidden',
                details: 'API returned 403 Forbidden',
                httpStatus: 403,
                accessDenied: true,
                suggestions: [
                    'Your credentials may not have permission to access this endpoint'
                ]
            };
        }

        // Save and return the response without any modification
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
            connectionSuccessful: true,
            fetchStrategy: 'direct-user-request',
            totalRecords: this.extractRecordCount(response.data),
            userPaginationRespected: true
        };

        // Save API data
        const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
        const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

        await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
        await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

        console.log(`✅ Direct user request data saved: ${dataFilePath}`);

        return {
            success: true,
            dataId: dataId,
            metadata: metadata,
            dataPreview: this.createPreview(response.data),
            message: `Direct API request successful - ${metadata.totalRecords} records retrieved as specified by user parameters`,
            connectionStatus: 'SUCCESS',
            authenticationStatus: 'success',
            userPaginationRespected: true,
            fetchStrategy: 'direct-user-request'
        };

    } catch (error) {
        console.error('Direct user request failed:', error.message);
        throw error;
    }
}

// NEW: Utility method to extract record count from any API response
extractRecordCount(data) {
    if (Array.isArray(data)) {
        return data.length;
    } else if (data && typeof data === 'object') {
        // Try common data field names
        const dataFields = ['data', 'result', 'results', 'items', 'records', 'entries', 'objects', 'answers'];
        for (const field of dataFields) {
            if (data[field] && Array.isArray(data[field])) {
                return data[field].length;
            }
        }

        // Single object response
        return 1;
    }
    return 0;
}
    // EXISTING: Smart fetch: Get total count + sample data for efficient comparison
    async fetchSmartSample(config) {
        try {
            console.log('=== SMART SAMPLE FETCH: GET TOTAL COUNT + REPRESENTATIVE SAMPLE ===');
            console.log(`Base URL: ${config.url}`);

            // Step 1: Get total count by fetching first page to understand structure
            const firstPageUrl = new URL(config.url);
            const firstPageParams = this.detectPaginationParams(config.url);

            // Set small sample size for first page
            Object.keys(firstPageParams).forEach(key => {
                if (key.includes('limit') || key.includes('per_page') || key.includes('size')) {
                    firstPageUrl.searchParams.set(key, '100'); // Get reasonable sample size
                } else if (key.includes('page') || key.includes('offset')) {
                    firstPageUrl.searchParams.set(key, firstPageParams[key]);
                } else {
                    firstPageUrl.searchParams.set(key, firstPageParams[key]);
                }
            });

            console.log(`Fetching first page for structure analysis: ${firstPageUrl.toString()}`);

            const firstPageResult = await this.fetchSinglePage({
                ...config,
                url: firstPageUrl.toString(),
                _skipPagination: true
            });

            if (!firstPageResult.success) {
                throw new Error(`Failed to fetch first page: ${firstPageResult.error}`);
            }

            const firstPageData = firstPageResult.data;

            // Extract total count and structure info
            let totalRecordsAvailable = 0;
            let sampleRecords = [];
            let paginationInfo = null;

            if (firstPageData.result && Array.isArray(firstPageData.result)) {
                sampleRecords = firstPageData.result;
                paginationInfo = firstPageData.result_info;
                totalRecordsAvailable = paginationInfo?.total_count || paginationInfo?.totalCount || sampleRecords.length;
            } else if (Array.isArray(firstPageData)) {
                sampleRecords = firstPageData;
                totalRecordsAvailable = sampleRecords.length; // No pagination info available
            } else if (firstPageData.data && Array.isArray(firstPageData.data)) {
                sampleRecords = firstPageData.data;
                paginationInfo = firstPageData.pagination || firstPageData.meta;
                totalRecordsAvailable = paginationInfo?.total_count || paginationInfo?.totalCount || sampleRecords.length;
            }

            console.log(`Total records available in API: ${totalRecordsAvailable}`);
            console.log(`Sample records fetched: ${sampleRecords.length}`);

            // Step 2: Decide strategy based on total count
            let finalResponse;
            let fetchStrategy;

            if (totalRecordsAvailable <= 500) {
                // Small dataset - fetch all records
                console.log('📊 SMALL DATASET: Fetching all records');
                fetchStrategy = 'complete';
                finalResponse = await this.fetchAllPagesOptimized(config, totalRecordsAvailable);
            } else if (totalRecordsAvailable <= 2000) {
                // Medium dataset - fetch first few pages for good sample
                console.log('📊 MEDIUM DATASET: Fetching representative sample (first 3 pages)');
                fetchStrategy = 'medium-sample';
                finalResponse = await this.fetchMultiplePages(config, 3);
            } else {
                // Large dataset - use smart sampling strategy
                console.log('📊 LARGE DATASET: Using smart sampling strategy');
                fetchStrategy = 'smart-sample';
                finalResponse = await this.fetchSmartSampleStrategy(config, totalRecordsAvailable);
            }

            // Enhance the response with total count information
            if (finalResponse.success) {
                finalResponse.metadata.totalRecordsAvailable = totalRecordsAvailable;
                finalResponse.metadata.fetchStrategy = fetchStrategy;
                finalResponse.metadata.sampleSize = finalResponse.metadata.totalRecords;
                finalResponse.metadata.samplePercentage = totalRecordsAvailable > 0 ?
                    ((finalResponse.metadata.totalRecords / totalRecordsAvailable) * 100).toFixed(1) + '%' : '100%';

                // Update the message to reflect smart sampling
                if (fetchStrategy === 'smart-sample' || fetchStrategy === 'medium-sample') {
                    finalResponse.message = `Smart sampling complete - ${finalResponse.metadata.totalRecords} sample records (${finalResponse.metadata.samplePercentage} of ${totalRecordsAvailable} total records)`;
                    finalResponse.samplingInfo = {
                        totalAvailable: totalRecordsAvailable,
                        sampleFetched: finalResponse.metadata.totalRecords,
                        samplePercentage: finalResponse.metadata.samplePercentage,
                        strategy: fetchStrategy,
                        isRepresentativeSample: true
                    };
                }
            }

            return finalResponse;

        } catch (error) {
            console.error('=== SMART SAMPLE FETCH FAILED ===');
            console.error(`Error: ${error.message}`);

            return {
                success: false,
                error: `Smart sample fetch failed: ${error.message}`,
                details: error.message,
                connectionStatus: 'FAILED'
            };
        }
    }

    // EXISTING: Fetch all pages for small datasets (≤500 records)
    async fetchAllPagesOptimized(config, totalCount) {
        const result = await this.fetchAllPages(config);
        if (result.success) {
            result.metadata.optimizationUsed = 'complete-fetch';
            result.metadata.reason = `Small dataset (${totalCount} records) - fetched all data`;
        }
        return result;
    }

    // EXISTING: Fetch multiple pages for medium datasets
    async fetchMultiplePages(config, maxPages) {
        try {
            const allResults = [];
            let currentPage = 1;

            while (currentPage <= maxPages) {
                console.log(`Fetching page ${currentPage} of ${maxPages}...`);

                const pageUrl = new URL(config.url);
                const paginationParams = this.detectPaginationParams(config.url, currentPage);

                Object.keys(paginationParams).forEach(key => {
                    pageUrl.searchParams.set(key, paginationParams[key]);
                });

                const pageResult = await this.fetchSinglePage({
                    ...config,
                    url: pageUrl.toString(),
                    _skipPagination: true
                });

                if (!pageResult.success) break;

                const pageData = pageResult.data;
                let currentPageResults = [];

                if (pageData.result && Array.isArray(pageData.result)) {
                    currentPageResults = pageData.result;
                } else if (Array.isArray(pageData)) {
                    currentPageResults = pageData;
                } else if (pageData.data && Array.isArray(pageData.data)) {
                    currentPageResults = pageData.data;
                }

                if (currentPageResults.length > 0) {
                    allResults.push(...currentPageResults);
                    console.log(`Page ${currentPage}: Found ${currentPageResults.length} records`);
                }

                currentPage++;
                if (currentPageResults.length === 0) break;
            }

            // Create response in appropriate format
            const combinedResponse = this.formatCombinedResponse(allResults, config);

            const dataId = uuidv4();
            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: 200,
                statusText: 'OK',
                contentType: 'application/json',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(combinedResponse).length,
                duration: 0,
                dataType: `Multi-page Sample (${allResults.length} records from ${maxPages} pages)`,
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true,
                totalRecords: allResults.length,
                pagesFetched: maxPages
            };

            await this.saveCombinedData(dataId, combinedResponse, metadata);

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: this.createPreview(combinedResponse),
                message: `Multi-page sample fetched - ${allResults.length} records from ${maxPages} pages`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success'
            };

        } catch (error) {
            return {
                success: false,
                error: `Multi-page fetch failed: ${error.message}`,
                connectionStatus: 'FAILED'
            };
        }
    }

    // EXISTING: Smart sampling strategy for large datasets
    async fetchSmartSampleStrategy(config, totalCount) {
        try {
            console.log(`🧠 SMART SAMPLING: ${totalCount} total records detected`);

            const allResults = [];
            const samplePages = [1, 2, 3]; // Always get first 3 pages

            // Add middle and end samples for very large datasets
            if (totalCount > 10000) {
                const totalPages = Math.ceil(totalCount / 100);
                const middlePage = Math.floor(totalPages / 2);
                const endPage = Math.max(totalPages - 1, middlePage + 1);

                samplePages.push(middlePage, endPage);
                console.log(`Adding middle (${middlePage}) and end (${endPage}) page samples`);
            }

            // Remove duplicates and sort
            const uniquePages = [...new Set(samplePages)].sort((a, b) => a - b);
            console.log(`Fetching sample pages: [${uniquePages.join(', ')}]`);

            for (const pageNum of uniquePages) {
                console.log(`Fetching sample page ${pageNum}...`);

                const pageUrl = new URL(config.url);
                const paginationParams = this.detectPaginationParams(config.url, pageNum);

                Object.keys(paginationParams).forEach(key => {
                    pageUrl.searchParams.set(key, paginationParams[key]);
                });

                const pageResult = await this.fetchSinglePage({
                    ...config,
                    url: pageUrl.toString(),
                    _skipPagination: true
                });

                if (pageResult.success) {
                    const pageData = pageResult.data;
                    let currentPageResults = [];

                    if (pageData.result && Array.isArray(pageData.result)) {
                        currentPageResults = pageData.result;
                    } else if (Array.isArray(pageData)) {
                        currentPageResults = pageData;
                    } else if (pageData.data && Array.isArray(pageData.data)) {
                        currentPageResults = pageData.data;
                    }

                    if (currentPageResults.length > 0) {
                        allResults.push(...currentPageResults);
                        console.log(`Page ${pageNum}: Added ${currentPageResults.length} records`);
                    }
                }

                // Small delay between requests
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            console.log(`Smart sampling complete: ${allResults.length} sample records from ${uniquePages.length} pages`);

            // Create response
            const combinedResponse = this.formatCombinedResponse(allResults, config);

            const dataId = uuidv4();
            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: 200,
                statusText: 'OK',
                contentType: 'application/json',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(combinedResponse).length,
                duration: 0,
                dataType: `Smart Sample (${allResults.length} records from ${uniquePages.length} strategic pages)`,
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true,
                totalRecords: allResults.length,
                samplePages: uniquePages,
                samplingStrategy: 'strategic-pages'
            };

            await this.saveCombinedData(dataId, combinedResponse, metadata);

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: this.createPreview(combinedResponse),
                message: `Smart sampling complete - ${allResults.length} representative records from ${uniquePages.length} strategic pages`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success'
            };

        } catch (error) {
            return {
                success: false,
                error: `Smart sampling failed: ${error.message}`,
                connectionStatus: 'FAILED'
            };
        }
    }

    // EXISTING: Detect pagination parameters from URL and API structure
    detectPaginationParams(url, pageNumber = 1) {
        const urlObj = new URL(url);
        const existingParams = Object.fromEntries(urlObj.searchParams.entries());

        // Check if URL already has pagination parameters and use the same pattern
        if (existingParams.limit !== undefined) {
            return {
                limit: 100,
                offset: (pageNumber - 1) * 100
            };
        } else if (existingParams.per_page !== undefined) {
            return {
                per_page: 100,
                page: pageNumber
            };
        } else if (existingParams.sysparm_limit !== undefined) {
            return {
                sysparm_limit: 100,
                sysparm_offset: (pageNumber - 1) * 100
            };
        } else {
            // Generic detection - try common patterns
            return {
                limit: 100,
                offset: (pageNumber - 1) * 100,
                page: pageNumber,
                per_page: 100
            };
        }
    }

    // EXISTING: Format combined response maintaining original structure
    formatCombinedResponse(allResults, config) {
        // Maintain original API response structure
        if (allResults.length > 0) {
            // Check if original response had wrapper structure
            return {
                success: true,
                errors: [],
                messages: [],
                result: allResults,
                result_info: {
                    total_count: allResults.length,
                    per_page: allResults.length,
                    page: 1
                }
            };
        } else {
            return allResults;
        }
    }

    // EXISTING: Save combined data and metadata
    async saveCombinedData(dataId, combinedResponse, metadata) {
        const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
        const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

        await fs.writeFile(dataFilePath, JSON.stringify(combinedResponse, null, 2));
        await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

        console.log(`✅ Smart sample data saved: ${dataFilePath}`);
    }

    // EXISTING: Fetch all pages from paginated API
    async fetchAllPages(config) {
        try {
            console.log('=== FETCHING ALL PAGES FROM PAGINATED API ===');
            console.log(`Base URL: ${config.url}`);

            const allResults = [];
            let currentPage = 1;
            let hasMorePages = true;
            let totalPages = 1;
            let totalRecordsFromAPI = 0;

            while (hasMorePages) {
                console.log(`Fetching page ${currentPage}...`);

                // Add page parameter to URL with dynamic parameter detection
                const pageUrl = new URL(config.url);
                const paginationParams = this.getPaginationParams(config.url, currentPage);

                // Add all pagination parameters to URL
                Object.keys(paginationParams).forEach(key => {
                    pageUrl.searchParams.set(key, paginationParams[key]);
                });

                const pageConfig = {
                    ...config,
                    url: pageUrl.toString()
                };

                // Fetch current page using existing fetchAPIData method (but avoid infinite recursion)
                const tempConfig = { ...pageConfig };
                tempConfig._skipPagination = true; // Flag to prevent infinite recursion

                const pageResult = await this.fetchSinglePage(tempConfig);

                if (!pageResult.success) {
                    console.error(`Failed to fetch page ${currentPage}:`, pageResult.error);
                    break;
                }

                const pageData = pageResult.data || {};

                // Handle different API response structures
                let currentPageResults = [];
                let paginationInfo = null;

                // Try different response formats
                if (pageData.result && Array.isArray(pageData.result)) {
                    // Cloudflare/ServiceNow style
                    currentPageResults = pageData.result;
                    paginationInfo = pageData.result_info;
                    if (paginationInfo && paginationInfo.total_count) {
                        totalRecordsFromAPI = paginationInfo.total_count;
                    }
                } else if (Array.isArray(pageData)) {
                    // Direct array response
                    currentPageResults = pageData;
                } else if (pageData.data && Array.isArray(pageData.data)) {
                    // Generic API style with data wrapper
                    currentPageResults = pageData.data;
                    paginationInfo = pageData.pagination || pageData.meta;
                } else if (typeof pageData === 'object' && pageData !== null) {
                    // Single object response
                    currentPageResults = [pageData];
                }

                if (currentPageResults.length > 0) {
                    allResults.push(...currentPageResults);
                    console.log(`Page ${currentPage}: Found ${currentPageResults.length} records (Total so far: ${allResults.length})`);

                    // Check pagination info from various sources
                    if (paginationInfo) {
                        totalPages = paginationInfo.total_pages || paginationInfo.totalPages || 1;
                        const totalCount = paginationInfo.total_count || paginationInfo.totalCount || 0;

                        console.log(`Pagination info - Current: ${currentPage}, Total pages: ${totalPages}, Total records: ${totalCount}`);

                        hasMorePages = currentPage < totalPages && currentPageResults.length > 0;
                    } else {
                        // No pagination info, use heuristic based on page size
                        const expectedPageSize = this.getExpectedPageSize(config.url);
                        hasMorePages = currentPageResults.length >= expectedPageSize;
                        console.log(`No pagination info - using heuristic: got ${currentPageResults.length}, expected ${expectedPageSize}, hasMore: ${hasMorePages}`);
                    }
                } else {
                    console.log(`Page ${currentPage}: No results found, stopping pagination`);
                    hasMorePages = false;
                }

                currentPage++;

                // Safety limit to prevent infinite loops
                if (currentPage > 1000) {
                    console.warn('Safety limit reached: stopping at 1000 pages');
                    break;
                }

                // Small delay between requests to be API-friendly
                if (hasMorePages) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }

            console.log(`=== PAGINATION COMPLETE ===`);
            console.log(`Total pages fetched: ${currentPage - 1}`);
            console.log(`Total records collected: ${allResults.length}`);
            if (totalRecordsFromAPI > 0) {
                console.log(`API reported total records: ${totalRecordsFromAPI}`);
            }

            // Create combined response maintaining original structure
            let combinedResponse;

            // Determine response format based on first page structure
            if (allResults.length > 0) {
                const testUrl = new URL(config.url);
                const firstPageParams = this.getPaginationParams(config.url, 1);
                Object.keys(firstPageParams).forEach(key => {
                    testUrl.searchParams.set(key, Math.min(firstPageParams[key], 1));
                });

                try {
                    const sampleResult = await this.fetchSinglePage({...config, url: testUrl.toString(), _skipPagination: true});
                    if (sampleResult.success && sampleResult.data && sampleResult.data.result !== undefined) {
                        // API uses wrapper format
                        combinedResponse = {
                            success: true,
                            errors: [],
                            messages: [],
                            result: allResults,
                            result_info: {
                                total_count: totalRecordsFromAPI || allResults.length,
                                total_pages: currentPage - 1,
                                per_page: allResults.length,
                                page: 1
                            }
                        };
                    } else {
                        // Direct array format
                        combinedResponse = allResults;
                    }
                } catch {
                    // Fallback to array format
                    combinedResponse = allResults;
                }
            } else {
                combinedResponse = allResults;
            }

            // Save the combined data using existing logic
            const dataId = uuidv4();
            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: 200,
                statusText: 'OK',
                contentType: 'application/json',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(combinedResponse).length,
                duration: 0,
                dataType: `Paginated API Response (${allResults.length} records from ${currentPage - 1} pages)`,
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true,
                paginationUsed: true,
                totalPages: currentPage - 1,
                totalRecords: allResults.length,
                apiReportedTotal: totalRecordsFromAPI
            };

            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            await fs.writeFile(dataFilePath, JSON.stringify(combinedResponse, null, 2));
            await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: this.createPreview(combinedResponse),
                message: `All pages fetched successfully - ${allResults.length} total records from ${currentPage - 1} pages`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success',
                paginationInfo: {
                    totalPages: currentPage - 1,
                    totalRecords: allResults.length,
                    apiReportedTotal: totalRecordsFromAPI,
                    paginationUsed: true
                }
            };

        } catch (error) {
            console.error('=== PAGINATION FETCH FAILED ===');
            console.error(`Error: ${error.message}`);

            return {
                success: false,
                error: `Pagination fetch failed: ${error.message}`,
                details: error.message,
                connectionStatus: 'FAILED'
            };
        }
    }

    // EXISTING: Fetch single page without pagination (used internally to avoid recursion)
    async fetchSinglePage(config) {
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
                    requestConfig.data = config.body;
                }
            }
        }

        const response = await axios(requestConfig);

        return {
            success: true,
            data: response.data,
            status: response.status,
            headers: response.headers
        };
    }

    // EXISTING: Smart sample fetch for comparison
    async fetchSmartSample(config) {
        try {
            console.log('=== SMART SAMPLE FETCH: GET TOTAL COUNT + SAMPLE FOR COMPARISON ===');
            console.log(`Base URL: ${config.url}`);

            // Step 1: Get first page with maximum reasonable size
            const firstPageUrl = new URL(config.url);
            const firstPageParams = this.detectPaginationParams(config.url);

            // Set reasonable sample size (50-100 records)
            Object.keys(firstPageParams).forEach(key => {
                if (key.includes('limit') || key.includes('per_page') || key.includes('size')) {
                    firstPageUrl.searchParams.set(key, '100'); // Get good sample size
                } else {
                    firstPageUrl.searchParams.set(key, firstPageParams[key]);
                }
            });

            console.log(`Fetching sample page: ${firstPageUrl.toString()}`);

            const firstPageResult = await this.fetchSinglePage({
                ...config,
                url: firstPageUrl.toString(),
                _skipPagination: true
            });

            if (!firstPageResult.success) {
                throw new Error(`Failed to fetch sample page: ${firstPageResult.error}`);
            }

            const firstPageData = firstPageResult.data;

            // Extract total count and sample records
            let totalRecordsAvailable = 0;
            let sampleRecords = [];
            let paginationInfo = null;

            // Handle different API response structures
            if (firstPageData.result && Array.isArray(firstPageData.result)) {
                // ServiceNow/Cloudflare style
                sampleRecords = firstPageData.result;
                paginationInfo = firstPageData.result_info;
                totalRecordsAvailable = paginationInfo?.total_count || sampleRecords.length;
            } else if (firstPageData.incidents && Array.isArray(firstPageData.incidents)) {
                // PagerDuty specific structure
                sampleRecords = firstPageData.incidents;
                totalRecordsAvailable = firstPageData.total || sampleRecords.length;
                paginationInfo = { total_count: totalRecordsAvailable };
            } else if (Array.isArray(firstPageData)) {
                // Direct array response
                sampleRecords = firstPageData;
                totalRecordsAvailable = sampleRecords.length;
            } else if (firstPageData.data && Array.isArray(firstPageData.data)) {
                // Generic wrapper
                sampleRecords = firstPageData.data;
                paginationInfo = firstPageData.pagination || firstPageData.meta;
                totalRecordsAvailable = paginationInfo?.total_count || paginationInfo?.totalCount || sampleRecords.length;
            }

            console.log(`✅ SMART SAMPLING RESULTS:`);
            console.log(`   Total Records Available: ${totalRecordsAvailable}`);
            console.log(`   Sample Records Fetched: ${sampleRecords.length}`);
            console.log(`   Sample Percentage: ${totalRecordsAvailable > 0 ? ((sampleRecords.length / totalRecordsAvailable) * 100).toFixed(1) : 100}%`);

            // Create response that preserves both total count AND sample data
            const combinedResponse = this.formatSmartSampleResponse(sampleRecords, firstPageData, totalRecordsAvailable);

            const dataId = uuidv4();
            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: 200,
                statusText: 'OK',
                contentType: 'application/json',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(combinedResponse).length,
                duration: 0,
                dataType: `Smart Sample API Response`,
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true,

                // KEY: Separate total vs sample counts
                totalRecordsAvailable: totalRecordsAvailable,  // 93 total
                sampleRecordsFetched: sampleRecords.length,    // 20 sample
                totalRecords: sampleRecords.length,            // For compatibility

                fetchStrategy: 'smart-sample',
                samplePercentage: totalRecordsAvailable > 0 ?
                    ((sampleRecords.length / totalRecordsAvailable) * 100).toFixed(1) + '%' : '100%',
                isRepresentativeSample: true,
                paginationInfo: paginationInfo
            };

            await this.saveCombinedData(dataId, combinedResponse, metadata);

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: this.createEnhancedPreview(combinedResponse, metadata),
                message: `Smart sampling - ${sampleRecords.length} sample records for comparison (${totalRecordsAvailable} total records available in API)`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success',
                samplingInfo: {
                    totalAvailable: totalRecordsAvailable,
                    sampleFetched: sampleRecords.length,
                    samplePercentage: metadata.samplePercentage,
                    strategy: 'efficient-sample',
                    isRepresentativeSample: true,
                    comparisonNote: `Comparing ${sampleRecords.length} sample records against BigQuery table for efficiency`
                }
            };

        } catch (error) {
            console.error('Smart sample fetch failed:', error.message);
            return {
                success: false,
                error: `Smart sample fetch failed: ${error.message}`,
                connectionStatus: 'FAILED'
            };
        }
    }

    // EXISTING: Format response with both total count and sample data
    formatSmartSampleResponse(sampleRecords, originalResponse, totalCount) {
        if (originalResponse && originalResponse.result !== undefined) {
            // ServiceNow/Cloudflare style
            return {
                ...originalResponse,
                result: sampleRecords,
                result_info: {
                    ...originalResponse.result_info,
                    total_count: totalCount,
                    sample_count: sampleRecords.length,
                    is_sample: true
                }
            };
        } else if (originalResponse && originalResponse.incidents !== undefined) {
            // PagerDuty specific
            return {
                ...originalResponse,
                incidents: sampleRecords,
                total: totalCount,
                sample_count: sampleRecords.length,
                is_sample: true
            };
        } else {
            // Generic array with metadata
            return {
                data: sampleRecords,
                total_count: totalCount,
                sample_count: sampleRecords.length,
                is_sample: true
            };
        }
    }

    // NEW: Create preview with total vs sample information
    createEnhancedPreview(data, metadata) {
        const preview = this.createPreview(data);

        // Enhance preview with total vs sample info
        if (metadata.totalRecordsAvailable && metadata.sampleRecordsFetched) {
            preview.totalRecordsAvailable = metadata.totalRecordsAvailable;
            preview.sampleRecordsFetched = metadata.sampleRecordsFetched;
            preview.samplePercentage = metadata.samplePercentage;
            preview.isSample = true;
            preview.samplingStrategy = metadata.fetchStrategy;
        }

        return preview;
    }

    // EXISTING: Get generic limit parameters for connection test
    getGenericLimitParams(url) {
        const urlLower = url.toLowerCase();

        // ServiceNow APIs
        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return { sysparm_limit: 1 };
        }
        // GitHub API
        else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return { per_page: 1 };
        }
        // Cloudflare API
        else if (urlLower.includes('api.cloudflare.com')) {
            return { per_page: 1 };
        }
        // REST APIs that commonly use 'limit'
        else if (urlLower.includes('/api/') || urlLower.includes('/rest/')) {
            return { limit: 1 };
        }
        // GraphQL or other APIs
        else if (urlLower.includes('graphql')) {
            return { first: 1 };
        }
        // Generic fallback - try multiple common parameters
        else {
            return {
                limit: 1,
                size: 1,
                count: 1,
                per_page: 1,
                pageSize: 1
            };
        }
    }

    // EXISTING: Get appropriate pagination parameters for different API types
    getPaginationParams(url, pageNumber) {
        const urlLower = url.toLowerCase();

        // ServiceNow APIs
        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return {
                sysparm_limit: 100,
                sysparm_offset: (pageNumber - 1) * 100
            };
        }
        // GitHub API
        else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return {
                per_page: 100,
                page: pageNumber
            };
        }
        // Cloudflare API
        else if (urlLower.includes('api.cloudflare.com')) {
            return {
                per_page: 100,
                page: pageNumber
            };
        }
        // REST APIs with standard pagination
        else if (urlLower.includes('/api/') || urlLower.includes('/rest/')) {
            return {
                limit: 100,
                offset: (pageNumber - 1) * 100,
                page: pageNumber
            };
        }
        // APIs that use 'size' parameter
        else if (urlLower.includes('elastic') || urlLower.includes('search')) {
            return {
                size: 100,
                from: (pageNumber - 1) * 100
            };
        }
        // Generic fallback - standard page/per_page
        else {
            return {
                page: pageNumber,
                per_page: 100,
                limit: 100,
                pageSize: 100
            };
        }
    }

    // EXISTING: Get expected page size for different API types
    getExpectedPageSize(url) {
        const urlLower = url.toLowerCase();

        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return 100;
        } else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return 100;
        } else if (urlLower.includes('api.cloudflare.com')) {
            return 100;
        } else {
            return 100; // Default page size
        }
    }

    // EXISTING: Helper method to get response preview for debugging
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

    // EXISTING: Get error suggestions based on response issues
    getResponseErrorSuggestions(reason, httpStatus) {
        const suggestions = [];

        switch (reason) {
            case 'EMPTY_RESPONSE':
                suggestions.push('Authentication is working - table appears empty');
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

    // EXISTING: Get API data from saved file
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

    // EXISTING: Get API data preview
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

    // EXISTING: Cleanup API data files
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

    // EXISTING: Get authentication type description
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

    // EXISTING: Configure authentication for requests
    configureAuthentication(requestConfig, config) {
        // Basic Authentication
        if (config.username && config.password) {
            const credentials = Buffer.from(`${config.username}:${config.password}`).toString('base64');
            requestConfig.headers['Authorization'] = `Basic ${credentials}`;
            console.log('Authentication configured');
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
            console.log('Custom headers configured');
        }
    }

    // EXISTING: Check if response contains error content
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

    // EXISTING: Extract error details from response
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

    // EXISTING: Detect data type for metadata
    detectDataType(data) {
        if (Array.isArray(data)) {
            return `Array (${data.length} items)`;
        } else if (typeof data === 'object' && data !== null) {
            if (data.result && Array.isArray(data.result)) {
                return `API Response (${data.result.length} records)`;
            } else if (data.result) {
                return 'API Response (single record)';
            } else {
                return `Object (${Object.keys(data).length} properties)`;
            }
        } else {
            return typeof data;
        }
    }

    // EXISTING: Create preview from API data
    createPreview(data) {
        try {
            let records = [];
            let totalRecords = 0;
            let format = 'Unknown';

            // Handle different API response formats
            if (typeof data === 'object' && data !== null && data.result) {
                if (Array.isArray(data.result)) {
                    records = data.result;
                    totalRecords = data.result.length;
                    format = 'API Response';
                } else {
                    records = [data.result];
                    totalRecords = 1;
                    format = 'API Response';
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

    // EXISTING: Flatten nested objects for field analysis
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

    // EXISTING: Format data size for display
    formatDataSize(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
}

module.exports = APIFetcherService;