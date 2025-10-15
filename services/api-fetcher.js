// services/api-fetcher.js - COMPLETE FIXED VERSION WITH PROPER TOTAL VS COMPARISON LOGIC
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class APIFetcherService {
    constructor() {
        this.tempDir = path.join(__dirname, '..', 'temp-api-data');
        this.ensureTempDirectory();

        // CONFIGURABLE PARAMETERS - No more hardcoding
        this.config = {
            // Threshold strategy configuration
            recordThreshold: process.env.API_RECORD_THRESHOLD || 5000,
            minSampleSize: process.env.API_MIN_SAMPLE_SIZE || 100,
            maxSampleSize: process.env.API_MAX_SAMPLE_SIZE || 10000,
            minSamplePercentage: process.env.API_MIN_SAMPLE_PERCENTAGE || 22,
            maxSamplePercentage: process.env.API_MAX_SAMPLE_PERCENTAGE || 22,
            maxSingleRequestSize: process.env.API_MAX_SINGLE_REQUEST || 1000,

            // Timeout and request configuration
            defaultTimeout: process.env.API_DEFAULT_TIMEOUT || 60000,
            maxRedirects: process.env.API_MAX_REDIRECTS || 5,
            requestDelay: process.env.API_REQUEST_DELAY || 100,
            maxPages: process.env.API_MAX_PAGES || 1000
        };

        console.log('Enhanced API Fetcher Service initialized with configurable parameters');
        console.log('Configuration:', this.config);
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

    // COMPLETELY FIXED: Main fetch method that properly handles total vs comparison records
    async fetchAPIData(config) {
        try {
            console.log('=== ENHANCED API DATA FETCH WITH FIXED TOTAL VS COMPARISON LOGIC ===');
            console.log(`URL: ${config.url}`);
            console.log(`Method: ${config.method || 'GET'}`);
            console.log(`Auth: ${this.getAuthType(config)}`);

            // PRIORITY CHECK: Detect if user specified pagination parameters
            const url = new URL(config.url);
            const hasUserPaginationParams = this.detectUserPaginationParams(url);

            if (hasUserPaginationParams) {
                console.log('USER PAGINATION DETECTED - Using direct request strategy');
                return await this.handleDirectUserRequest(config, url);
            }

            // Check for specific strategy requests from frontend
            if (config.fetchAllWithFirstPageComparison) {
                console.log('ALL RECORDS + FIRST PAGE COMPARISON strategy requested');
                return await this.fetchAllRecordsForComparison(config);
            }

            if (config.useSmartSampling) {
                console.log('SMART SAMPLING strategy requested');
                return await this.fetchSmartSampleWithProperCounts(config);
            }

            if (config.fetchAllPages) {
                console.log('FETCH ALL PAGES strategy requested');
                return await this.fetchAllPages(config);
            }

            // DEFAULT STRATEGY: Always try to get total count first, then decide on comparison sample
            console.log('Using default strategy with proper total vs comparison handling');
            return await this.fetchWithProperTotalAndComparisonLogic(config);

        } catch (error) {
            console.error('fetchAPIData error:', error);
            return this.createErrorResponse(error);
        }
    }

    // NEW: Default strategy with proper total vs comparison logic
    async fetchWithProperTotalAndComparisonLogic(config) {
        try {
            console.log('=== FETCHING WITH PROPER TOTAL VS COMPARISON LOGIC ===');

            // Step 1: Make initial request to get structure and potential total count
            const requestConfig = this.buildRequestConfig(config);

            // Try to get a reasonable sample to understand the API structure
            const initialUrl = new URL(config.url);
            this.applyGenericPaginationParams(initialUrl, 1, 100); // Get first 100 records
            requestConfig.url = initialUrl.toString();

            console.log(`Initial request URL: ${requestConfig.url}`);

            const startTime = Date.now();
            const response = await axios(requestConfig);
            const duration = Date.now() - startTime;

            console.log(`Initial request completed: ${response.status} in ${duration}ms`);

            // Validate response
            const validationResult = this.isValidJSONResponse(response);
            if (!validationResult.isValid) {
                throw new Error(`Invalid response: ${validationResult.details}`);
            }

            // Step 2: Extract total count and sample data
            const totalRecordsInAPI = this.extractTotalCountGeneric(response) || this.countRecordsInResponse(response.data);
            const sampleRecords = this.extractRecordsFromResponse(response.data);
            const sampleSize = sampleRecords.length;

            console.log(`DETECTED: ${totalRecordsInAPI} total records in API`);
            console.log(`SAMPLE: Got ${sampleSize} records for analysis`);

            // Step 3: Determine comparison strategy based on total count
            let recordsForComparison = sampleSize;
            let comparisonStrategy = 'complete-sample';
            let isCompleteFetch = false;

            if (totalRecordsInAPI <= this.config.recordThreshold) {
                // Small dataset - try to get all records for comparison
                console.log(`STRATEGY: Fetch all records for comparison (${totalRecordsInAPI} <= ${this.config.recordThreshold})`);

                if (totalRecordsInAPI > sampleSize) {
                    // Need to fetch more records
                    try {
                        const allRecordsUrl = new URL(config.url);
                        this.applyGenericPaginationParams(allRecordsUrl, 1, totalRecordsInAPI);
                        const allRecordsConfig = { ...requestConfig, url: allRecordsUrl.toString() };

                        const allResponse = await axios(allRecordsConfig);
                        if (this.isValidJSONResponse(allResponse).isValid) {
                            const allRecords = this.extractRecordsFromResponse(allResponse.data);
                            recordsForComparison = allRecords.length;
                            response.data = allResponse.data; // Use complete data
                            comparisonStrategy = 'complete-dataset';
                            isCompleteFetch = true;
                            console.log(`SUCCESS: Fetched all ${recordsForComparison} records for comparison`);
                        }
                    } catch (error) {
                        console.log(`Failed to fetch all records, using sample: ${error.message}`);
                        // Keep the original sample
                    }
                } else {
                    comparisonStrategy = 'complete-dataset';
                    isCompleteFetch = true;
                }
            } else {
    // Large dataset - use sample for comparison
    const sampleConfig = this.calculateDynamicSampleSize(totalRecordsInAPI);
    comparisonStrategy = 'representative-sample';

    console.log(`STRATEGY: Use 22% sample for comparison (${totalRecordsInAPI} > ${this.config.recordThreshold})`);
    console.log(`CALCULATED: Need ${sampleConfig.sampleSize} records (${sampleConfig.actualPercentage}% of total)`);
    console.log(`CURRENT: Have ${sampleSize} records from initial fetch`);

    // FIXED: Always fetch the optimized sample if it's different from what we have
    if (sampleConfig.sampleSize !== sampleSize) {
        console.log(`FETCHING OPTIMIZED SAMPLE: Requesting ${sampleConfig.sampleSize} records...`);
        try {
            const sampleUrl = new URL(config.url);
            this.applyGenericPaginationParams(sampleUrl, 1, sampleConfig.sampleSize);
            const sampleRequestConfig = { ...requestConfig, url: sampleUrl.toString() };

            const sampleResponse = await axios(sampleRequestConfig);
            if (this.isValidJSONResponse(sampleResponse).isValid) {
                const newSampleRecords = this.extractRecordsFromResponse(sampleResponse.data);
                recordsForComparison = newSampleRecords.length;
                response.data = sampleResponse.data; // Use optimized sample
                console.log(`✅ SUCCESS: Fetched optimized sample of ${recordsForComparison} records`);
            }
        } catch (error) {
            console.log(`⚠️ Failed to fetch optimized sample, using original ${sampleSize} records: ${error.message}`);
            recordsForComparison = sampleSize; // Keep the original sample
        }
    } else {
        // Sample size matches what we need
        recordsForComparison = sampleSize;
        console.log(`✅ USING INITIAL SAMPLE: ${sampleSize} records is sufficient`);
    }
}
            // Step 4: Create proper metadata with correct total vs comparison counts
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

                // FIXED: Proper separation of total vs comparison counts
                totalRecords: totalRecordsInAPI,  // ACTUAL total from API
                totalRecordsInAPI: totalRecordsInAPI,  // ACTUAL total from API
                totalRecordsAvailable: totalRecordsInAPI,  // ACTUAL total from API
                recordsForComparison: recordsForComparison,  // Records used for comparison

                fetchStrategy: comparisonStrategy,
                isCompleteFetch: isCompleteFetch,
                comparisonCoverage: isCompleteFetch ? '100%' : `${((recordsForComparison / totalRecordsInAPI) * 100).toFixed(2)}%`,
                comparisonStrategy: isCompleteFetch ? 'complete-dataset' : 'representative-sample',
                userPaginationRespected: false
            };

            // Save data and metadata
            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
            await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

            // Create enhanced preview
            const dataPreview = this.createEnhancedPreview(response.data, {
                totalRecordsAvailable: totalRecordsInAPI,
                sampleSize: recordsForComparison,
                strategy: comparisonStrategy,
                isComplete: isCompleteFetch
            });

            const message = isCompleteFetch
                ? `Complete dataset - ${totalRecordsInAPI} total records, all ${recordsForComparison} used for comparison`
                : `Efficient strategy - ${totalRecordsInAPI} total records in API, ${recordsForComparison} used for comparison (${((recordsForComparison / totalRecordsInAPI) * 100).toFixed(2)}% sample)`;

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: dataPreview,
                message: message,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success',
                fetchStrategy: comparisonStrategy,
                totalRecordsInAPI: totalRecordsInAPI,  // ACTUAL total
                recordsRetrieved: recordsForComparison,  // For comparison
                comparisonAccuracy: isCompleteFetch ? 'COMPLETE' : 'REPRESENTATIVE',
                userPaginationRespected: false
            };

        } catch (error) {
            console.error('fetchWithProperTotalAndComparisonLogic error:', error);
            return this.createErrorResponse(error);
        }
    }

    // ENHANCED: Generic user pagination detection
    detectUserPaginationParams(url) {
        const commonPaginationParams = [
            // Page-based pagination
            'page', 'p', 'pageNumber', 'pageNum', 'pageIndex',
            // Limit-based pagination
            'limit', 'size', 'count', 'per_page', 'perPage', 'pageSize', 'page_size',
            // Offset-based pagination
            'offset', 'skip', 'start', 'from',
            // API-specific but common
            'sysparm_limit', 'sysparm_offset', 'max_results', 'maxResults',
            // Cursor-based pagination
            'cursor', 'next', 'continuation_token', 'after', 'before'
        ];

        return commonPaginationParams.some(param => url.searchParams.has(param));
    }

    // NEW: Enhanced generic total count extraction
    extractTotalCountGeneric(response) {
        try {
            console.log('Extracting total count with enhanced detection...');

            // 1. Check response headers
            const headerKeys = ['x-total-count', 'x-total', 'total-count', 'total-records'];
            for (const headerKey of headerKeys) {
                if (response.headers[headerKey]) {
                    const value = parseInt(response.headers[headerKey]);
                    if (!isNaN(value) && value > 0) {
                        console.log(`Total count from header ${headerKey}: ${value}`);
                        return value;
                    }
                }
            }

            // 2. Search response data recursively
            if (response.data && typeof response.data === 'object') {
                const totalCount = this.findTotalCountInObject(response.data);
                if (totalCount !== null) {
                    console.log(`Total count from response data: ${totalCount}`);
                    return totalCount;
                }
            }

            // 3. Count array elements if direct array
            if (Array.isArray(response.data)) {
                console.log(`Total count from direct array: ${response.data.length}`);
                return response.data.length;
            }

            console.log('No reliable total count found, will use sample size');
            return null;

        } catch (error) {
            console.error('Error extracting total count:', error.message);
            return null;
        }
    }

    // NEW: Recursive total count finder
    findTotalCountInObject(obj, depth = 0) {
        if (depth > 3) return null; // Prevent deep recursion

        // Check direct properties that might contain total count
        const totalCountKeys = [
            'total', 'count', 'totalCount', 'total_count', 'totalRecords', 'total_records',
            'totalElements', 'totalSize', 'size', 'length', 'num_results', 'numResults'
        ];

        for (const key of totalCountKeys) {
            if (obj[key] !== undefined) {
                const value = parseInt(obj[key]);
                if (!isNaN(value) && value > 0) {
                    return value;
                }
            }
        }

        // Check nested objects that might contain pagination info
        const paginationKeys = [
            'pagination', 'paging', 'meta', 'metadata', 'result_info', 'resultInfo',
            'page_info', 'pageInfo', 'response_metadata', 'info'
        ];

        for (const key of paginationKeys) {
            if (obj[key] && typeof obj[key] === 'object') {
                const nestedResult = this.findTotalCountInObject(obj[key], depth + 1);
                if (nestedResult !== null) {
                    return nestedResult;
                }
            }
        }

        return null;
    }

 calculateDynamicSampleSize(totalRecords) {
    const targetPercentage = 22;

    // Calculate 10% of total records
    let sampleSize = Math.ceil(totalRecords * 0.22);

    // Apply minimum constraint (at least 100 records)
    sampleSize = Math.max(this.config.minSampleSize, sampleSize);

    // Apply maximum constraint if configured (default 10000)
    sampleSize = Math.min(this.config.maxSampleSize, sampleSize);

    const actualPercentage = ((sampleSize / totalRecords) * 100).toFixed(1);

    console.log(`SAMPLE CALCULATION: ${totalRecords} total records → ${sampleSize} sample (${actualPercentage}%)`);

    return {
        sampleSize,
        targetPercentage,
        actualPercentage,
        strategy: totalRecords > 10000 ? 'large-dataset' : 'medium-dataset'
    };
}
    // NEW: Generic pagination parameter application
    applyGenericPaginationParams(url, page, size) {
        // Apply common pagination patterns
        // The API will ignore unsupported parameters

        // Page + per_page pattern
        url.searchParams.set('page', page.toString());
        url.searchParams.set('per_page', size.toString());

        // Limit + offset pattern
        url.searchParams.set('limit', size.toString());
        url.searchParams.set('offset', ((page - 1) * size).toString());

        // Size-based patterns
        url.searchParams.set('size', size.toString());
        url.searchParams.set('pageSize', size.toString());
        url.searchParams.set('page_size', size.toString());

        // Common API-specific patterns
        url.searchParams.set('sysparm_limit', size.toString());
        url.searchParams.set('sysparm_offset', ((page - 1) * size).toString());
        url.searchParams.set('max_results', size.toString());
        url.searchParams.set('maxResults', size.toString());

        return {
            page: page,
            size: size,
            offset: (page - 1) * size
        };
    }

    // NEW: Extract records from any response structure
    extractRecordsFromResponse(data) {
        if (Array.isArray(data)) {
            return data;
        }

        if (data && typeof data === 'object') {
            // Find the largest array - likely the main data
            let largestArray = [];
            let largestCount = 0;

            const findLargestArray = (obj, depth = 0) => {
                if (depth > 3) return;

                for (const [key, value] of Object.entries(obj)) {
                    if (Array.isArray(value) && value.length > largestCount) {
                        largestArray = value;
                        largestCount = value.length;
                    } else if (value && typeof value === 'object') {
                        findLargestArray(value, depth + 1);
                    }
                }
            };

            findLargestArray(data);
            return largestArray.length > 0 ? largestArray : [data];
        }

        return [data];
    }

    // NEW: Count records in any response structure
    countRecordsInResponse(data) {
        if (Array.isArray(data)) {
            return data.length;
        }

        if (data && typeof data === 'object') {
            // Find the largest array in the response
            let largestCount = 0;

            const findArrays = (obj, depth = 0) => {
                if (depth > 3) return;

                for (const [key, value] of Object.entries(obj)) {
                    if (Array.isArray(value) && value.length > largestCount) {
                        largestCount = value.length;
                    } else if (value && typeof value === 'object') {
                        findArrays(value, depth + 1);
                    }
                }
            };

            findArrays(data);
            return largestCount;
        }

        return 1; // Single object
    }

    // NEW: Enhanced preview creation
    createEnhancedPreview(data, enhancementData = {}) {
        const preview = this.createPreview(data);

        // Add enhancement information
        if (enhancementData.totalRecordsAvailable) {
            preview.totalRecordsAvailable = enhancementData.totalRecordsAvailable;
            preview.sampleSize = enhancementData.sampleSize;
            preview.strategy = enhancementData.strategy;
            preview.isComplete = enhancementData.isComplete;
            preview.isSample = !enhancementData.isComplete;
        }

        return preview;
    }

    // NEW: Build enhanced request configuration
    buildRequestConfig(config) {
        const requestConfig = {
            method: config.method || 'GET',
            url: config.url,
            timeout: this.config.defaultTimeout,
            maxRedirects: this.config.maxRedirects,
            validateStatus: function (status) {
                return status < 500;
            },
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'ETL-Validation-Dashboard/2.0',
                ...config.defaultHeaders
            }
        };

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

        return requestConfig;
    }

    // FIXED: Smart sampling with proper total vs comparison counts
    async fetchSmartSampleWithProperCounts(config) {
        try {
            console.log('=== SMART SAMPLE WITH PROPER TOTAL VS COMPARISON COUNTS ===');

            // Make initial request to understand the data structure
            const requestConfig = this.buildRequestConfig(config);
            const firstPageUrl = new URL(config.url);
            this.applyGenericPaginationParams(firstPageUrl, 1, 100);
            requestConfig.url = firstPageUrl.toString();

            console.log(`Smart sample URL: ${requestConfig.url}`);

            const startTime = Date.now();
            const response = await axios(requestConfig);
            const duration = Date.now() - startTime;

            const validationResult = this.isValidJSONResponse(response);
            if (!validationResult.isValid) {
                throw new Error(`Invalid response: ${validationResult.details}`);
            }

            // Extract total count and sample data
            const totalRecordsInAPI = this.extractTotalCountGeneric(response) || this.countRecordsInResponse(response.data);
            const sampleRecords = this.extractRecordsFromResponse(response.data);
            const recordsForComparison = sampleRecords.length;

            console.log(`SMART SAMPLE: ${totalRecordsInAPI} total records, ${recordsForComparison} for comparison`);

            // Create proper metadata
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

                // FIXED: Proper total vs comparison separation
                totalRecords: totalRecordsInAPI,  // ACTUAL total from API
                totalRecordsAvailable: totalRecordsInAPI,  // ACTUAL total from API
                totalRecordsInAPI: totalRecordsInAPI,  // ACTUAL total from API
                recordsForComparison: recordsForComparison,  // Sample used for comparison
                fetchStrategy: 'smart-sample-with-proper-counts',
                userPaginationRespected: false
            };

            // Save data and metadata
            const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
            const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

            await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
            await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

            const dataPreview = this.createEnhancedPreview(response.data, {
                totalRecordsAvailable: totalRecordsInAPI,
                sampleSize: recordsForComparison,
                strategy: 'smart-sample'
            });

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: dataPreview,
                message: `Smart sampling - ${totalRecordsInAPI} total records in API, ${recordsForComparison} records used for comparison`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success',
                userPaginationRespected: false
            };

        } catch (error) {
            console.error('fetchSmartSampleWithProperCounts error:', error);
            return this.createErrorResponse(error);
        }
    }

    // PRESERVED: All existing methods remain exactly the same

    // Direct user request handler - PRESERVED
    async handleDirectUserRequest(config, url) {
        console.log('Processing direct user request without modification...');

        const requestConfig = this.buildRequestConfig(config);
        requestConfig.url = config.url; // Use EXACT URL with user parameters

        console.log('Making DIRECT API request with user pagination...');
        const startTime = Date.now();
        const response = await axios(requestConfig);
        const duration = Date.now() - startTime;

        console.log(`Direct request completed: ${response.status} in ${duration}ms`);

        const validationResult = this.isValidJSONResponse(response);
        if (!validationResult.isValid) {
            throw new Error(`Invalid response: ${validationResult.details}`);
        }

        // Count records in response
      let recordCount = 0;
if (Array.isArray(response.data)) {
    recordCount = response.data.length;
} else if (response.data.result && Array.isArray(response.data.result)) {
    recordCount = response.data.result.length;
} else if (response.data.data && Array.isArray(response.data.data)) {
    recordCount = response.data.data.length;
} else if (response.data.answers && Array.isArray(response.data.answers)) {
    recordCount = response.data.answers.length;
} else if (response.data.issues && Array.isArray(response.data.issues)) {
    recordCount = response.data.issues.length;  // JIRA API support
} else if (response.data.items && Array.isArray(response.data.items)) {
    recordCount = response.data.items.length;   // Common API pattern
} else if (response.data.records && Array.isArray(response.data.records)) {
    recordCount = response.data.records.length; // Common API pattern
} else if (response.data.entries && Array.isArray(response.data.entries)) {
    recordCount = response.data.entries.length; // Common API pattern
} else {
    recordCount = 1;
}

console.log(`RECORD COUNT DETECTION: Found ${recordCount} records`);
if (response.data.issues) {
    console.log(`JIRA API: issues array contains ${response.data.issues.length} items`);
}
        // Try to determine total available
        let totalAvailableEstimate = recordCount;
        let isPartialResult = false;

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

        const userPaginationParams = Object.fromEntries(url.searchParams.entries());
        const paginationKeys = ['per_page', 'limit', 'page_size', 'sysparm_limit', 'page', 'offset'];
        const detectedPaginationParams = Object.keys(userPaginationParams).filter(key =>
            paginationKeys.includes(key)
        );

        console.log(`USER PAGINATION SUCCESS: Got ${recordCount} records as requested`);

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
            totalRecords: recordCount,
            totalRecordsInAPI: totalAvailableEstimate,
            totalRecordsAvailable: totalAvailableEstimate,
            recordsForComparison: recordCount,
            fetchStrategy: 'direct-user-request',
            userPaginationRespected: true,
            userPaginationParams: userPaginationParams,
            detectedPaginationParams: detectedPaginationParams,
            isPartialResult: isPartialResult
        };

        const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
        const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

        await fs.writeFile(dataFilePath, JSON.stringify(response.data, null, 2));
        await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

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

    // PRESERVED: Enhanced error response creation
    createErrorResponse(error) {
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

    // ALL EXISTING METHODS PRESERVED EXACTLY AS THEY WERE:

    isValidJSONResponse(response) {
        try {
            console.log(`=== ENHANCED RESPONSE VALIDATION ===`);
            console.log(`Status: ${response.status}`);
            console.log(`Content-Type: ${response.headers['content-type'] || 'unknown'}`);

            const contentType = response.headers['content-type'] || '';

            if (contentType.toLowerCase().includes('text/html')) {
                console.log('Content-Type indicates HTML response');
                return {
                    isValid: false,
                    reason: 'HTML_CONTENT_TYPE',
                    details: `API returned Content-Type: ${contentType} (expected application/json)`
                };
            }

            if (typeof response.data === 'object' && response.data !== null) {
                console.log('Response is already a valid JSON object');
                return { isValid: true };
            }

            if (typeof response.data === 'string') {
                const responseStr = response.data;
                console.log(`String response length: ${responseStr.length} characters`);

                const trimmedResponse = responseStr.trim();
                if (trimmedResponse === '') {
                    console.log('Empty string response');
                    return {
                        isValid: false,
                        reason: 'EMPTY_RESPONSE',
                        details: 'API returned empty string response'
                    };
                }

                if (trimmedResponse.length < 2) {
                    console.log('Response too short');
                    return {
                        isValid: false,
                        reason: 'RESPONSE_TOO_SHORT',
                        details: `Response only ${trimmedResponse.length} characters: "${trimmedResponse}"`
                    };
                }

                if (trimmedResponse.startsWith('<!DOCTYPE') ||
                    trimmedResponse.startsWith('<html') ||
                    trimmedResponse.includes('<title>')) {
                    console.log('String response contains HTML');
                    return {
                        isValid: false,
                        reason: 'HTML_CONTENT',
                        details: 'Response contains HTML markup instead of JSON'
                    };
                }

                try {
                    console.log('Attempting to parse JSON string...');
                    console.log(`First 50 chars: "${trimmedResponse.substring(0, 50)}"`);
                    console.log(`Last 50 chars: "${trimmedResponse.slice(-50)}"`);

                    const looksLikeJSON = (trimmedResponse.startsWith('{') && trimmedResponse.endsWith('}')) ||
                                        (trimmedResponse.startsWith('[') && trimmedResponse.endsWith(']'));

                    if (!looksLikeJSON) {
                        console.log('Response does not look like JSON');
                        return {
                            isValid: false,
                            reason: 'NOT_JSON_FORMAT',
                            details: `Response does not start/end with JSON brackets`
                        };
                    }

                    const parsed = JSON.parse(trimmedResponse);
                    console.log('JSON parsing successful');
                    response.data = parsed;
                    return { isValid: true, parsedData: parsed };

                } catch (parseError) {
                    console.log(`JSON parsing failed: ${parseError.message}`);

                    if (parseError.message.includes('Unexpected end of JSON input')) {
                        const lastChar = trimmedResponse.slice(-1);
                        const startsWithBracket = trimmedResponse.startsWith('{') || trimmedResponse.startsWith('[');
                        const endsWithBracket = lastChar === '}' || lastChar === ']';

                        if (startsWithBracket && !endsWithBracket) {
                            return {
                                isValid: false,
                                reason: 'TRUNCATED_JSON',
                                details: `JSON response appears truncated`
                            };
                        } else {
                            return {
                                isValid: false,
                                reason: 'INCOMPLETE_JSON',
                                details: `JSON response is incomplete or corrupted`
                            };
                        }
                    } else if (parseError.message.includes('Unexpected token')) {
                        return {
                            isValid: false,
                            reason: 'JSON_SYNTAX_ERROR',
                            details: `JSON syntax error: ${parseError.message}`
                        };
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

    // FIXED: Enhanced testAPIConnection method in api-fetcher.js
// Replace the existing testAPIConnection method with this improved version

async testAPIConnection(config) {
    try {
        console.log('Testing API connection with enhanced validation...');
        console.log(`URL: ${config.url}`);
        console.log(`Auth: ${this.getAuthType(config)}`);

        // FIXED: Determine API type for specialized handling
        const urlLower = config.url.toLowerCase();
        const isServiceNow = urlLower.includes('service-now') || urlLower.includes('servicenow');
        const isCloudflare = urlLower.includes('cloudflare.com');
        const isGitHub = urlLower.includes('github.com') || urlLower.includes('api.github.com');

        const testConfig = {
            method: 'GET', // FIXED: Always use GET for connection test, not HEAD
            url: config.url,
            timeout: 30000,
            maxRedirects: 5,
            validateStatus: function (status) {
                return status < 500;
            },
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'ETL-Validation-Dashboard/2.0'
            }
        };

        this.configureAuthentication(testConfig, config);

        // FIXED: Add API-specific parameters only where needed
        if (isServiceNow) {
            testConfig.params = { sysparm_limit: 1 };
        } else if (isCloudflare) {
            // Cloudflare API doesn't need pagination params for connection test
            // Just test the endpoint as-is
        } else if (isGitHub) {
            testConfig.params = { per_page: 1 };
        }
        // FIXED: Don't add generic pagination params for unknown APIs

        const startTime = Date.now();
        let response;

        try {
            console.log(`Making GET request to: ${testConfig.url}`);
            response = await axios(testConfig);
        } catch (requestError) {
            console.log('Direct request failed, analyzing error...');
            throw requestError; // Don't try alternative requests for non-ServiceNow APIs
        }

        const duration = Date.now() - startTime;
        console.log(`Request completed: ${response.status} in ${duration}ms`);

        // FIXED: Improved empty response handling
        const hasEmptyResponse = (
            (typeof response.data === 'string' && response.data.trim() === '') ||
            (response.data === null) ||
            (typeof response.data === 'object' && Object.keys(response.data).length === 0)
        );

        if (hasEmptyResponse && isServiceNow) {
            // ONLY try alternative tables for ServiceNow
            console.log('Empty response in ServiceNow - trying alternative tables...');

            const alternativeUrl = config.url.replace(/\/table\/[^/?]+/, '/table/incident');
            console.log(`Testing alternative ServiceNow table: ${alternativeUrl}`);

            try {
                const altTestConfig = { ...testConfig };
                altTestConfig.url = alternativeUrl;
                altTestConfig.params = { sysparm_limit: 1 };

                const altResponse = await axios(altTestConfig);

                if (altResponse.data && typeof altResponse.data === 'object') {
                    console.log('Alternative table has data - original table appears empty');
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
                            'The original table appears to be empty or inaccessible',
                            'Try these ServiceNow tables with data: incident, sys_user, cmdb_ci',
                            'Or add query parameters to your current URL: ?sysparm_limit=10'
                        ]
                    };
                }
            } catch (altError) {
                console.log('Alternative table test failed:', altError.message);
            }
        } else if (hasEmptyResponse && !isServiceNow) {
            // FIXED: For non-ServiceNow APIs with empty response, check if it's actually successful
            if (response.status >= 200 && response.status < 300) {
                console.log('Empty response but successful status code - may be normal for this API endpoint');
                return {
                    success: true,
                    connectionSuccessful: true,
                    authenticationSuccessful: true,
                    status: response.status,
                    statusText: response.statusText,
                    duration: duration,
                    authType: this.getAuthType(config),
                    contentType: response.headers['content-type'] || 'unknown',
                    message: 'Connection and authentication successful',
                    authMessage: 'Authentication successful',
                    warning: 'API returned empty response (may be normal for this endpoint)',
                    suggestions: [
                        'Connection and authentication are working correctly',
                        'The API endpoint responded successfully but with no data',
                        'This may be normal behavior for this specific endpoint',
                        'Try fetching data to verify the connection is fully functional'
                    ]
                };
            }
        }

        // FIXED: Enhanced response validation
        const validationResult = this.isValidJSONResponse(response);

        if (!validationResult.isValid && response.status >= 200 && response.status < 300) {
            // FIXED: For successful status codes with invalid JSON, still consider connection successful
            console.log(`Non-JSON response but successful HTTP status: ${response.status}`);

            return {
                success: true,
                connectionSuccessful: true,
                authenticationSuccessful: true,
                status: response.status,
                statusText: response.statusText,
                duration: duration,
                authType: this.getAuthType(config),
                contentType: response.headers['content-type'] || 'unknown',
                message: 'Connection and authentication successful',
                authMessage: 'Authentication successful',
                warning: 'Response format may not be standard JSON',
                suggestions: [
                    'Connection and authentication are working correctly',
                    'API responded with successful status code',
                    'Response format may be different than expected, but connection is valid',
                    'Proceed with data fetching to test full functionality'
                ]
            };
        }

        if (!validationResult.isValid) {
            console.log(`Invalid response detected: ${validationResult.reason}`);

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

        // FIXED: Standard success response
        const connectionSuccessful = response.status >= 200 && response.status < 300;
        const authenticationSuccessful = response.status !== 401 && response.status !== 403;
        const overallSuccess = connectionSuccessful || (response.status === 401 || response.status === 403);

        return {
            success: overallSuccess,
            connectionSuccessful: connectionSuccessful,
            authenticationSuccessful: authenticationSuccessful,
            status: response.status,
            statusText: response.statusText,
            duration: duration,
            authType: this.getAuthType(config),
            contentType: response.headers['content-type'] || 'unknown',
            message: connectionSuccessful ? 'Connection successful' : `API returned ${response.status} - ${response.statusText}`,
            authMessage: authenticationSuccessful ? 'Authentication successful' : 'Authentication failed',

            ...(response.status === 400 && {
                error: 'Bad Request - API endpoint or parameters may be incorrect',
                suggestions: [
                    'Check if the API endpoint URL is complete and correct',
                    'Verify if this endpoint requires specific query parameters',
                    'Some APIs require POST requests instead of GET for authentication',
                    'Try a different endpoint for testing'
                ]
            })
        };

    } catch (error) {
        console.error('Connection test failed:', error);
        console.error('Error response:', error.response?.data);
        console.error('Error response status:', error.response?.status);
        console.error('Error response headers:', error.response?.headers);

        let errorMessage = 'Connection test failed';
        let authenticationFailed = false;

        if (error.response) {
            const status = error.response.status;
            const responseData = error.response.data;

            // FIXED: Better error data extraction
            let errorDetails = '';
            if (responseData) {
                if (typeof responseData === 'string') {
                    errorDetails = responseData;
                } else if (responseData.error) {
                    errorDetails = typeof responseData.error === 'string' ?
                        responseData.error : JSON.stringify(responseData.error);
                } else if (responseData.errors && Array.isArray(responseData.errors)) {
                    errorDetails = responseData.errors.map(e => e.message || JSON.stringify(e)).join(', ');
                } else if (responseData.message) {
                    errorDetails = responseData.message;
                } else {
                    errorDetails = JSON.stringify(responseData).substring(0, 200);
                }
            }

            if (status === 400) {
                console.log('400 Bad Request details:', responseData);
                errorMessage = `Bad Request (400): ${errorDetails || 'The API endpoint or parameters are incorrect'}`;
                authenticationFailed = false;
            } else if (status === 401) {
                errorMessage = `Authentication Failed (401 Unauthorized)${errorDetails ? ': ' + errorDetails : ''}`;
                authenticationFailed = true;
            } else if (status === 403) {
                errorMessage = `Access Forbidden (403 Forbidden)${errorDetails ? ': ' + errorDetails : ''}`;
                authenticationFailed = true;
            } else if (status >= 500) {
                errorMessage = `Server Error (${status}): ${errorDetails || 'API server is experiencing issues'}`;
                authenticationFailed = false;
            } else {
                errorMessage = `HTTP ${status}: ${error.response.statusText || 'Request failed'}${errorDetails ? ' - ' + errorDetails : ''}`;
            }
        } else if (error.code === 'ECONNREFUSED') {
            errorMessage = 'Connection Refused - API server not reachable';
        } else if (error.code === 'ETIMEDOUT') {
            errorMessage = 'Connection Timeout - API server too slow';
        } else if (error.code === 'ENOTFOUND') {
            errorMessage = 'DNS Error - API server hostname not found';
        } else {
            errorMessage = error.message || error.toString() || 'Unknown connection error';
        }

        return {
            success: false,
            connectionSuccessful: false,
            authenticationSuccessful: !authenticationFailed,
            error: errorMessage,
            details: error.message || 'No details available',
            httpStatus: error.response?.status || 'NO_RESPONSE',
            statusText: error.response?.statusText || 'Unknown',
            responseData: error.response?.data || null,
            authType: this.getAuthType(config)
        };
    }
}
    // ALL OTHER EXISTING METHODS REMAIN EXACTLY THE SAME
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
                    'User-Agent': 'ETL-Validation-Dashboard/2.0'
                }
            };

            this.configureAuthentication(requestConfig, config);

            const firstPageUrl = new URL(config.url);

            if (config.url.includes('service-now') || config.url.includes('servicenow')) {
                firstPageUrl.searchParams.set('sysparm_limit', '100');
                firstPageUrl.searchParams.set('sysparm_offset', '0');
            } else if (config.url.includes('github.com/api') || config.url.includes('api.github.com')) {
                firstPageUrl.searchParams.set('per_page', '100');
                firstPageUrl.searchParams.set('page', '1');
            } else {
                if (!firstPageUrl.searchParams.has('limit') && !firstPageUrl.searchParams.has('per_page')) {
                    firstPageUrl.searchParams.set('limit', '100');
                    firstPageUrl.searchParams.set('page', '1');
                }
            }

            requestConfig.url = firstPageUrl.toString();
            console.log(`First page URL: ${requestConfig.url}`);

            const response = await axios(requestConfig);

            const validationResult = this.isValidJSONResponse(response);
            if (!validationResult.isValid) {
                throw new Error(`Invalid first page response: ${validationResult.details}`);
            }

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

    formatFirstPageWithTotalCount(firstPageRecords, originalResponse, totalCount) {
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

    extractTotalCount(response) {
        try {
            if (response.headers['x-total-count']) {
                const headerCount = parseInt(response.headers['x-total-count']);
                if (!isNaN(headerCount)) {
                    console.log(`Total count from header x-total-count: ${headerCount}`);
                    return headerCount;
                }
            }

            const totalHeaders = ['x-total-count', 'x-total', 'total-count', 'total-records'];

            for (const headerName of totalHeaders) {
                if (response.headers[headerName]) {
                    const headerCount = parseInt(response.headers[headerName]);
                    if (!isNaN(headerCount)) {
                        console.log(`Total count from header ${headerName}: ${headerCount}`);
                        return headerCount;
                    }
                }
            }

            if (response.data && typeof response.data === 'object') {
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

                if (Array.isArray(response.data)) {
                    console.log(`Total count from array length: ${response.data.length}`);
                    return response.data.length;
                }

                if (response.data.result && Array.isArray(response.data.result)) {
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

            console.log('No total count information found in response');
            return null;

        } catch (error) {
            console.error('Error extracting total count:', error.message);
            return null;
        }
    }

    extractRecordCount(data) {
        if (Array.isArray(data)) {
            return data.length;
        } else if (data && typeof data === 'object') {
            const dataFields = ['data', 'result', 'results', 'items', 'records', 'entries', 'objects', 'answers'];
            for (const field of dataFields) {
                if (data[field] && Array.isArray(data[field])) {
                    return data[field].length;
                }
            }
            return 1;
        }
        return 0;
    }

    async fetchSmartSample(config) {
        // Use the new method with proper counts
        return await this.fetchSmartSampleWithProperCounts(config);
    }

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

                const pageUrl = new URL(config.url);
                const paginationParams = this.getPaginationParams(config.url, currentPage);

                Object.keys(paginationParams).forEach(key => {
                    pageUrl.searchParams.set(key, paginationParams[key]);
                });

                const pageConfig = {
                    ...config,
                    url: pageUrl.toString()
                };

                const tempConfig = { ...pageConfig };
                tempConfig._skipPagination = true;

                const pageResult = await this.fetchSinglePage(tempConfig);

                if (!pageResult.success) {
                    console.error(`Failed to fetch page ${currentPage}:`, pageResult.error);
                    break;
                }

                const pageData = pageResult.data || {};

                let currentPageResults = [];
                let paginationInfo = null;

                if (pageData.result && Array.isArray(pageData.result)) {
                    currentPageResults = pageData.result;
                    paginationInfo = pageData.result_info;
                    if (paginationInfo && paginationInfo.total_count) {
                        totalRecordsFromAPI = paginationInfo.total_count;
                    }
                } else if (Array.isArray(pageData)) {
                    currentPageResults = pageData;
                } else if (pageData.data && Array.isArray(pageData.data)) {
                    currentPageResults = pageData.data;
                    paginationInfo = pageData.pagination || pageData.meta;
                } else if (typeof pageData === 'object' && pageData !== null) {
                    currentPageResults = [pageData];
                }

                if (currentPageResults.length > 0) {
                    allResults.push(...currentPageResults);
                    console.log(`Page ${currentPage}: Found ${currentPageResults.length} records (Total so far: ${allResults.length})`);

                    if (paginationInfo) {
                        totalPages = paginationInfo.total_pages || paginationInfo.totalPages || 1;
                        const totalCount = paginationInfo.total_count || paginationInfo.totalCount || 0;

                        console.log(`Pagination info - Current: ${currentPage}, Total pages: ${totalPages}, Total records: ${totalCount}`);

                        hasMorePages = currentPage < totalPages && currentPageResults.length > 0;
                    } else {
                        const expectedPageSize = this.getExpectedPageSize(config.url);
                        hasMorePages = currentPageResults.length >= expectedPageSize;
                        console.log(`No pagination info - using heuristic: got ${currentPageResults.length}, expected ${expectedPageSize}, hasMore: ${hasMorePages}`);
                    }
                } else {
                    console.log(`Page ${currentPage}: No results found, stopping pagination`);
                    hasMorePages = false;
                }

                currentPage++;

                if (currentPage > this.config.maxPages) {
                    console.warn(`Safety limit reached: stopping at ${this.config.maxPages} pages`);
                    break;
                }

                if (hasMorePages) {
                    await new Promise(resolve => setTimeout(resolve, this.config.requestDelay));
                }
            }

            console.log(`=== PAGINATION COMPLETE ===`);
            console.log(`Total pages fetched: ${currentPage - 1}`);
            console.log(`Total records collected: ${allResults.length}`);
            if (totalRecordsFromAPI > 0) {
                console.log(`API reported total records: ${totalRecordsFromAPI}`);
            }

            let combinedResponse;

            if (allResults.length > 0) {
                const testUrl = new URL(config.url);
                const firstPageParams = this.getPaginationParams(config.url, 1);
                Object.keys(firstPageParams).forEach(key => {
                    testUrl.searchParams.set(key, Math.min(firstPageParams[key], 1));
                });

                try {
                    const sampleResult = await this.fetchSinglePage({...config, url: testUrl.toString(), _skipPagination: true});
                    if (sampleResult.success && sampleResult.data && sampleResult.data.result !== undefined) {
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
                        combinedResponse = allResults;
                    }
                } catch {
                    combinedResponse = allResults;
                }
            } else {
                combinedResponse = allResults;
            }

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

    async fetchSinglePage(config) {
        const requestConfig = {
            method: config.method || 'GET',
            url: config.url,
            timeout: this.config.defaultTimeout,
            maxRedirects: this.config.maxRedirects,
            validateStatus: function (status) {
                return status < 500;
            },
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'ETL-Validation-Dashboard/2.0'
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

        const response = await axios(requestConfig);

        return {
            success: true,
            data: response.data,
            status: response.status,
            headers: response.headers
        };
    }

    getGenericLimitParams(url) {
        const urlLower = url.toLowerCase();

        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return { sysparm_limit: 1 };
        } else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return { per_page: 1 };
        } else if (urlLower.includes('api.cloudflare.com')) {
            return { per_page: 1 };
        } else if (urlLower.includes('/api/') || urlLower.includes('/rest/')) {
            return { limit: 1 };
        } else if (urlLower.includes('graphql')) {
            return { first: 1 };
        } else {
            return {
                limit: 1,
                size: 1,
                count: 1,
                per_page: 1,
                pageSize: 1
            };
        }
    }

    getPaginationParams(url, pageNumber) {
        const urlLower = url.toLowerCase();

        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return {
                sysparm_limit: 100,
                sysparm_offset: (pageNumber - 1) * 100
            };
        } else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return {
                per_page: 100,
                page: pageNumber
            };
        } else if (urlLower.includes('api.cloudflare.com')) {
            return {
                per_page: 100,
                page: pageNumber
            };
        } else if (urlLower.includes('/api/') || urlLower.includes('/rest/')) {
            return {
                limit: 100,
                offset: (pageNumber - 1) * 100,
                page: pageNumber
            };
        } else if (urlLower.includes('elastic') || urlLower.includes('search')) {
            return {
                size: 100,
                from: (pageNumber - 1) * 100
            };
        } else {
            return {
                page: pageNumber,
                per_page: 100,
                limit: 100,
                pageSize: 100
            };
        }
    }

    getExpectedPageSize(url) {
        const urlLower = url.toLowerCase();

        if (urlLower.includes('service-now') || urlLower.includes('servicenow')) {
            return 100;
        } else if (urlLower.includes('github.com/api') || urlLower.includes('api.github.com')) {
            return 100;
        } else if (urlLower.includes('api.cloudflare.com')) {
            return 100;
        } else {
            return 100;
        }
    }

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
    console.log('=== AUTHENTICATION DEBUG ===');
    console.log('Auth Type:', config.authType);
    console.log('Raw headers:', config.headers);

    if (config.username && config.password) {
        const credentials = Buffer.from(`${config.username}:${config.password}`).toString('base64');
        requestConfig.headers['Authorization'] = `Basic ${credentials}`;
        console.log('Basic Authentication configured');
    }

    if (config.headers && Object.keys(config.headers).length > 0) {
        console.log('Processing custom headers...');
        if (typeof config.headers === 'string') {
            console.log('Headers are string, parsing...');
            const headerLines = config.headers.split('\n');
            for (const line of headerLines) {
                const trimmedLine = line.trim();
                if (trimmedLine) {
                    const colonIndex = trimmedLine.indexOf(':');
                    if (colonIndex > 0) {
                        const key = trimmedLine.substring(0, colonIndex).trim();
                        const value = trimmedLine.substring(colonIndex + 1).trim();

                        // Skip Content-Type for GET requests
                        if (key.toLowerCase() === 'content-type' &&
                            (config.method || 'GET').toUpperCase() === 'GET') {
                            console.log('Skipping Content-Type header for GET request');
                            continue;
                        }

                        requestConfig.headers[key] = value;
                        console.log(`Added header: ${key} = ${value}`);
                    }
                }
            }
        } else {
            // Handle object headers
            Object.entries(config.headers).forEach(([key, value]) => {
                // Skip Content-Type for GET requests
                if (key.toLowerCase() === 'content-type' &&
                    (config.method || 'GET').toUpperCase() === 'GET') {
                    console.log('Skipping Content-Type header for GET request');
                    return;
                }
                requestConfig.headers[key] = value;
                console.log(`Added header: ${key} = ${value}`);
            });
        }
    }

    console.log('Final request headers:', requestConfig.headers);
    console.log('=== END AUTHENTICATION DEBUG ===');
}  // <-- THIS CLOSING BRACE WAS MISSING OR IN THE WRONG PLACE

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

    createPreview(data) {
        try {
            let records = [];
            let totalRecords = 0;
            let format = 'Unknown';

            if (typeof data === 'object' && data !== null) {
                const arrayFields = Object.keys(data).filter(key => Array.isArray(data[key]));

                if (arrayFields.length > 0) {
                    const mainArrayField = arrayFields.reduce((a, b) =>
                        data[a].length > data[b].length ? a : b
                    );

                    records = data[mainArrayField];
                    totalRecords = records.length;
                    format = `API Response (${mainArrayField} array)`;
                    console.log(`Detected main data array: ${mainArrayField} with ${totalRecords} records`);
                } else {
                    records = [data];
                    totalRecords = 1;
                    format = 'Single Object Response';
                }
            } else if (Array.isArray(data)) {
                records = data;
                totalRecords = data.length;
                format = 'Direct Array Response';
            }

            if (!records || records.length === 0 || !records[0]) {
                return {
                    totalRecords: totalRecords,
                    fieldsDetected: 0,
                    format: format,
                    error: 'No records found in API response',
                    availableFields: [],
                    idFields: [],
                    importantFields: []
                };
            }

            const firstRecord = records[0];
            const flattenedSample = this.flattenObject(firstRecord);
            const allFields = Object.keys(flattenedSample);

            const idFields = this.categorizeIdFields(allFields);
            const importantFields = this.categorizeImportantFields(allFields, idFields);

            return {
                totalRecords: totalRecords,
                fieldsDetected: allFields.length,
                fileSize: JSON.stringify(data).length,
                format: format,
                sampleRecords: [flattenedSample],
                availableFields: allFields,
                idFields: idFields,
                importantFields: importantFields,
                originalSample: firstRecord,
                allFieldsList: allFields
            };

        } catch (error) {
            console.error('Preview creation failed:', error.message);
            return {
                totalRecords: 0,
                fieldsDetected: 0,
                format: 'Error',
                error: `Preview generation failed: ${error.message}`,
                availableFields: [],
                idFields: [],
                importantFields: []
            };
        }
    }

    categorizeIdFields(fields) {
        return fields.filter(field => {
            const lowerField = field.toLowerCase();
            return lowerField.match(/.*id$/) ||
                   lowerField.match(/^id.*/) ||
                   lowerField.includes('_id') ||
                   lowerField.includes('key') ||
                   lowerField.includes('number') ||
                   lowerField.includes('identifier');
        });
    }

    categorizeImportantFields(fields, idFields) {
        return fields.filter(field => {
            if (idFields.includes(field)) return false;

            const lowerField = field.toLowerCase();
            return lowerField.includes('name') ||
                   lowerField.includes('title') ||
                   lowerField.includes('description') ||
                   lowerField.includes('status') ||
                   lowerField.includes('type') ||
                   lowerField.includes('category') ||
                   lowerField.includes('score') ||
                   lowerField.includes('value') ||
                   lowerField.includes('amount') ||
                   lowerField.includes('count') ||
                   lowerField.includes('active') ||
                   lowerField.includes('enabled');
        });
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

    // PRESERVED: All Records + First Page Comparison strategy
    async fetchAllRecordsForComparison(config) {
        try {
            console.log('=== ALL RECORDS + FIRST PAGE COMPARISON STRATEGY ===');
            console.log('This strategy fetches complete dataset but uses first page for comparison efficiency');

            const allRecordsResult = await this.fetchAllPages(config);

            if (!allRecordsResult.success) {
                throw new Error(`Failed to fetch all records: ${allRecordsResult.error}`);
            }

            const firstPageResult = await this.fetchFirstPageForComparison(config);

            if (!firstPageResult.success) {
                throw new Error(`Failed to fetch first page: ${firstPageResult.error}`);
            }

            const allRecordsData = allRecordsResult.dataPreview;
            const firstPageRecords = firstPageResult.records;
            const totalRecordsCount = allRecordsData.totalRecords;

            const formattedResponse = this.formatFirstPageWithTotalCount(
                firstPageRecords,
                firstPageResult.data,
                totalRecordsCount
            );

            const dataId = uuidv4();
            const metadata = {
                id: dataId,
                url: config.url,
                method: config.method || 'GET',
                status: 200,
                statusText: 'OK',
                contentType: 'application/json',
                fetchedAt: new Date().toISOString(),
                responseSize: JSON.stringify(formattedResponse).length,
                duration: 0,
                dataType: `All Records + First Page Comparison (${totalRecordsCount} total, ${firstPageRecords.length} for comparison)`,
                authenticationUsed: this.getAuthType(config),
                authenticationStatus: 'success',
                connectionSuccessful: true,

                totalRecords: firstPageRecords.length,
                totalRecordsInAPI: totalRecordsCount,
                totalRecordsAvailable: totalRecordsCount,
                recordsForComparison: firstPageRecords.length,
                fetchStrategy: 'all-records-first-page-comparison',
                hasCompleteDataset: true,
                comparisonStrategy: 'first-page-efficient',
                allRecordsDataId: allRecordsResult.dataId
            };

            await this.saveCombinedData(dataId, formattedResponse, metadata);

            const dataPreview = this.createEnhancedPreview(formattedResponse, {
                totalRecordsAvailable: totalRecordsCount,
                sampleSize: firstPageRecords.length,
                strategy: 'all-records-first-page-comparison',
                isComplete: false,
                hasCompleteDataset: true
            });

            return {
                success: true,
                dataId: dataId,
                metadata: metadata,
                dataPreview: dataPreview,
                message: `All Records + First Page strategy complete - ${firstPageRecords.length} records for comparison from ${totalRecordsCount} total records fetched`,
                connectionStatus: 'SUCCESS',
                authenticationStatus: 'success',
                fetchStrategy: 'all-records-first-page-comparison',
                comparisonInfo: {
                    totalRecordsInAPI: totalRecordsCount,
                    recordsUsedForComparison: firstPageRecords.length,
                    comparisonStrategy: 'first-page-efficient',
                    hasCompleteDataset: true,
                    completeDatasetId: allRecordsResult.dataId
                }
            };

        } catch (error) {
            console.error('All Records + First Page Comparison strategy failed:', error.message);
            return {
                success: false,
                error: `All Records + First Page strategy failed: ${error.message}`,
                connectionStatus: 'FAILED'
            };
        }
    }

    async saveCombinedData(dataId, combinedResponse, metadata) {
        const dataFilePath = path.join(this.tempDir, `${dataId}.json`);
        const metadataFilePath = path.join(this.tempDir, `${dataId}_metadata.json`);

        await fs.writeFile(dataFilePath, JSON.stringify(combinedResponse, null, 2));
        await fs.writeFile(metadataFilePath, JSON.stringify(metadata, null, 2));

        console.log(`Data saved: ${dataFilePath}`);
    }

    generateUUID() {
        return uuidv4();
    }
}

module.exports = APIFetcherService;