// ========================================
// API vs BQ - Global State & Dependencies
// ========================================

let connectionTestPassed = false;
let apiDataFetched = false;
let globalApiResults = null;
let currentApiDataId = null;

        function enableFetchDataButton() {
            const fetchButton = document.getElementById('fetchApiData');
            fetchButton.disabled = false;
            fetchButton.innerHTML = '📄 Fetch API Data';
            fetchButton.style.background = 'linear-gradient(135deg, #27ae60 0%, #229954 100%)';
            fetchButton.style.boxShadow = '0 4px 15px rgba(39, 174, 96, 0.3)';
            fetchButton.style.cursor = 'pointer';

            console.log('✅ Fetch API Data button enabled after successful connection test');
        }

        function disableFetchDataButton() {
            const fetchButton = document.getElementById('fetchApiData');
            fetchButton.disabled = true;
            fetchButton.innerHTML = `📄 Fetch API Data<div style="font-size: 0.8rem; margin-top: 4px; opacity: 0.9;">⚠️ Test connection first</div>`;
            fetchButton.style.background = 'linear-gradient(135deg, #bdc3c7 0%, #95a5a6 100%)';
            fetchButton.style.boxShadow = '0 2px 8px rgba(149, 165, 166, 0.3)';
            fetchButton.style.cursor = 'not-allowed';

            console.log('⚠️ Fetch API Data button disabled - connection test required');
        }

        // Initialize fetch button as disabled on page load
        document.addEventListener('DOMContentLoaded', function() {
            disableFetchDataButton();
            console.log('🔧 API section initialized - fetch button disabled until connection test passes');
        });


        function setAsApiPrimaryKey(fieldName) {
            const primaryKeyInput = document.getElementById('apiPrimaryKey');
            if (primaryKeyInput) {
                primaryKeyInput.value = fieldName;
                console.log('API primary key set to:', fieldName);

                primaryKeyInput.style.background = '#e8f5e8';
                primaryKeyInput.style.borderColor = '#27ae60';

                setTimeout(() => {
                    primaryKeyInput.style.background = '';
                    primaryKeyInput.style.borderColor = '#e9ecef';
                }, 1500);
            }
        }


        function startNewApiComparison() {
            // ENHANCED: Reset dependency states for API section
            connectionTestPassed = false;
            apiDataFetched = false;
            disableFetchDataButton();

            document.getElementById('api-comparison-results').style.display = 'none';
            document.getElementById('api-upload-section').style.display = 'block';
            document.getElementById('api-processing-section').style.display = 'none';
            document.getElementById('startApiComparison').disabled = true;
            document.getElementById('api-preview').style.display = 'none';

            // Clear all form fields
            document.getElementById('apiUrl').value = '';
            document.getElementById('apiUsername').value = '';
            document.getElementById('apiPassword').value = '';
            document.getElementById('apiKeyHeader').value = 'X-API-Key';
            document.getElementById('apiKeyValue').value = '';
            document.getElementById('bearerToken').value = '';
            document.getElementById('apiHeaders').value = '';
            document.getElementById('apiBody').value = '';
            document.getElementById('apiPrimaryKey').value = '';
            document.getElementById('apiBqTable').value = '';
            document.getElementById('apiBqFilter').value = ''; // Clear BigQuery filter
            document.getElementById('apiExplodeArrayField').value = ''; // Clear explode array field

            // Reset authentication type
            document.getElementById('authType').value = 'none';
            toggleAuthFields();

            // Hide connection test result
            document.getElementById('connectionTestResult').style.display = 'none';

            // Reset method to GET
            document.querySelectorAll('.method-option').forEach(opt => opt.classList.remove('selected'));
            document.querySelector('.method-option[data-method="GET"]').classList.add('selected');
            document.getElementById('apiBodySection').style.display = 'none';

            currentApiDataId = null;
            globalApiResults = null;

            const exportButton = document.getElementById('exportApiToExcel');
            if (exportButton) exportButton.disabled = true;

            console.log('API interface reset completely with connection dependency states');
        }


        function toggleAuthFields() {
    const authType = document.getElementById('authType').value;

    // Get the Additional Headers section (the entire .option-group containing apiHeaders)
    const apiHeadersElement = document.getElementById('apiHeaders');
    const apiHeadersGroup = apiHeadersElement ? apiHeadersElement.parentElement : null;

    // Hide all auth-specific fields first
    document.getElementById('basicAuthFields').style.display = 'none';
    document.getElementById('apiKeyFields').style.display = 'none';
    document.getElementById('bearerTokenFields').style.display = 'none';

    // Hide Additional Headers by default
    if (apiHeadersGroup) {
        apiHeadersGroup.style.display = 'none';
    }

    // Show relevant fields based on selected authentication type
    switch(authType) {
        case 'basic':
            // Show only username and password for Basic Auth
            document.getElementById('basicAuthFields').style.display = 'block';
            console.log('✓ Basic Authentication selected - Additional Headers HIDDEN');
            break;

        case 'apikey':
            // Show only API Key fields for API Key Auth
            document.getElementById('apiKeyFields').style.display = 'block';
            console.log('✓ API Key Authentication selected - Additional Headers HIDDEN');
            break;

        case 'bearer':
            // Show only Bearer Token field for Bearer Auth
            document.getElementById('bearerTokenFields').style.display = 'block';
            console.log('✓ Bearer Token Authentication selected - Additional Headers HIDDEN');
            break;

        case 'custom':
            // ONLY show Additional Headers for Custom Headers authentication
            if (apiHeadersGroup) {
                apiHeadersGroup.style.display = 'block';
            }
            console.log('✓ Custom Headers Only selected - Additional Headers SHOWN');
            break;

        case 'none':
            // No authentication - hide everything except URL
            console.log('✓ No Authentication selected - Additional Headers HIDDEN');
            break;
    }

    console.log('Authentication type changed to:', authType);
}

        // Build authentication configuration
        function buildAuthConfig() {
            const authType = document.getElementById('authType').value;
            const headers = {};
            let username = '';
            let password = '';

            switch(authType) {
                case 'basic':
                    username = document.getElementById('apiUsername').value.trim();
                    password = document.getElementById('apiPassword').value.trim();
                    break;

                case 'apikey':
                    const keyHeader = document.getElementById('apiKeyHeader').value.trim() || 'X-API-Key';
                    const keyValue = document.getElementById('apiKeyValue').value.trim();
                    if (keyValue) {
                        headers[keyHeader] = keyValue;
                    }
                    break;

                case 'bearer':
                    const token = document.getElementById('bearerToken').value.trim();
                    if (token) {
                        headers['Authorization'] = 'Bearer ' + token;
                    }
                    break;
            }

            return { headers, username, password, authType };
        }

        // API Method Selection
        // Replace the existing method option click handler with this
document.querySelectorAll('.method-option').forEach(option => {
    option.addEventListener('click', function() {
        // Check if method is disabled
        if (this.classList.contains('disabled')) {
            console.log('Method disabled:', this.getAttribute('data-method'));
            alert('PUT and PATCH methods are temporarily disabled. Please use GET or POST methods.');
            return; // Exit early, don't process disabled methods
        }

        // Normal processing for enabled methods
        document.querySelectorAll('.method-option').forEach(opt => opt.classList.remove('selected'));
        this.classList.add('selected');

        const method = this.getAttribute('data-method');
        const bodySection = document.getElementById('apiBodySection');
        const postAuthSection = document.getElementById('postAuthSection');

        if (['POST', 'PUT', 'PATCH'].includes(method)) {
            bodySection.style.display = 'block';
            if (method === 'POST') {
                postAuthSection.style.display = 'block'; // Show POST auth option
            }
        } else {
            bodySection.style.display = 'none';
            postAuthSection.style.display = 'none';
        }
    });
});
       // FIXED: Update test connection handler in index.html
document.getElementById('testApiConnection').addEventListener('click', async function() {
    const url = document.getElementById('apiUrl').value.trim();
    const method = document.querySelector('.method-option.selected').getAttribute('data-method'); // Get selected method

    if (!url) {
        alert('Please enter an API URL first!');
        return;
    }

    const button = this;
    const originalText = button.innerHTML;

    try {
        button.disabled = true;
        button.innerHTML = '🔄 Testing Connection...';

        const authConfig = buildAuthConfig();
        const headersText = document.getElementById('apiHeaders').value.trim();
        const bodyText = document.getElementById('apiBody').value.trim(); // Get body for POST

        // Parse additional headers
        const additionalHeaders = {};
        if (headersText) {
            const headerLines = headersText.split('\n');
            for (const line of headerLines) {
                const [key, ...valueParts] = line.split(':');
                if (key && valueParts.length > 0) {
                    additionalHeaders[key.trim()] = valueParts.join(':').trim();
                }
            }
        }

        const allHeaders = Object.assign({}, authConfig.headers, additionalHeaders);

        const testConfig = {
            url: url,
            method: method, // FIXED: Pass the selected method
            headers: allHeaders,
            username: authConfig.username,
            password: authConfig.password,
            authType: authConfig.authType,
            body: bodyText // FIXED: Include body for POST requests
        };

        console.log('Testing with method:', method);
        console.log('Request body:', bodyText);

        const response = await fetch('/api/test-api-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(testConfig)
        });

        const result = await response.json();

        const resultDiv = document.getElementById('connectionTestResult');
        resultDiv.style.display = 'block';

        const httpStatus = result.httpStatus || result.status;
        const isHttpSuccess = httpStatus >= 200 && httpStatus < 300;

        if (result.success && result.connectionSuccessful && result.authenticationSuccessful && isHttpSuccess) {
            connectionTestPassed = true;
            enableFetchDataButton();

            let successMessage = `
                <div style="background: #d4edda; color: #155724; padding: 15px; border-radius: 8px; border-left: 4px solid #28a745;">
                    <h5 style="margin-bottom: 10px;">✅ Connection & Authentication Successful!</h5>
                    <p><strong>Status:</strong> ${result.status} ${result.statusText}</p>
                    <p><strong>Method:</strong> ${method}</p>
                    <p><strong>Auth Type:</strong> ${result.authType}</p>
                    <p><strong>Response Time:</strong> ${result.duration}ms</p>
            `;

            // Add token info for POST authentication
            if (result.hasToken && method === 'POST') {
                successMessage += `
                    <p style="margin: 10px 0; padding: 10px; background: #fff; border-radius: 4px;">
                        <strong style="color: #28a745;">🔑 Bearer Token Received!</strong><br>
                        <span style="font-size: 0.9rem; color: #666;">Token successfully generated for data fetch requests</span>
                    </p>
                `;
            }

            successMessage += `
                    <p style="margin: 10px 0 0 0; font-weight: 600; color: #28a745;">🚀 Ready to fetch data!</p>
                </div>
            `;

            resultDiv.innerHTML = successMessage;
        } else {
            connectionTestPassed = false;
            disableFetchDataButton();

            let errorMessage = result.error || 'Connection test failed';
            let suggestions = [];

            if (httpStatus === 404) {
                suggestions = [
                    `Endpoint not found - verify the URL is correct`,
                    `For Peakon auth: ensure URL is /api/v1/auth/application`,
                    `Check API documentation for correct endpoint path`
                ];
            } else if (httpStatus === 400) {
                suggestions = [
                    `Bad Request - check your request body format`,
                    `For Peakon: body should be {"type": "bearer_tokens"}`,
                    `Verify Content-Type header is application/json`
                ];
            } else if (httpStatus === 401) {
                suggestions = [
                    `Authentication failed - check credentials`,
                    `Verify API key or token is correct`,
                    `Check if additional headers are required`
                ];
            }

            resultDiv.innerHTML = `
                <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 8px; border-left: 4px solid #dc3545;">
                    <h5 style="margin-bottom: 10px;">❌ Connection Failed</h5>
                    <p><strong>Error:</strong> ${errorMessage}</p>
                    <p><strong>HTTP Status:</strong> ${httpStatus}</p>
                    <p><strong>Method:</strong> ${method}</p>
                    ${suggestions.length > 0 ? `
                        <div style="margin-top: 15px; padding: 10px; background: #fff3cd; border-radius: 4px;">
                            <strong>💡 Suggestions:</strong>
                            <ul style="margin: 5px 0 0 20px; padding: 0;">
                                ${suggestions.map(s => `<li style="margin: 5px 0;">${s}</li>`).join('')}
                            </ul>
                        </div>
                    ` : ''}
                </div>
            `;
        }

    } catch (error) {
        console.error('Test failed:', error);
        connectionTestPassed = false;
        disableFetchDataButton();

        const resultDiv = document.getElementById('connectionTestResult');
        resultDiv.style.display = 'block';
        resultDiv.innerHTML = `
            <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 8px;">
                <h5>❌ Test Failed</h5>
                <p>${error.message}</p>
            </div>
        `;
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
});

        // ENHANCED: Fetch API Data with Dependency Check
        document.getElementById('fetchApiData').addEventListener('click', async function() {
            // ENHANCED: PREVENT FETCH IF CONNECTION NOT TESTED
            if (!connectionTestPassed) {
                alert('⚠️ Please test API connection & authentication first!\n\nUse the "Test API Connection" button before fetching data.');
                return;
            }

    const url = document.getElementById('apiUrl').value.trim();
    const method = document.querySelector('.method-option.selected').getAttribute('data-method');
    const headersText = document.getElementById('apiHeaders').value.trim();
    const bodyText = document.getElementById('apiBody').value.trim();

    // NEW: Get POST authentication settings
    const requiresPostAuth = document.getElementById('requiresPostAuth')?.checked || false;
    const dataFetchUrl = document.getElementById('dataFetchUrl')?.value.trim() || '';

    if (!url) {
        alert('Please enter an API URL!');
        return;
    }

    const button = this;
    const originalText = button.innerHTML;

    try {
        button.disabled = true;
        button.innerHTML = 'ðŸ"„ Fetching API Data...';

        document.getElementById('api-upload-section').style.display = 'none';
        document.getElementById('api-processing-section').style.display = 'block';

        // Update processing text for POST auth
        if (requiresPostAuth) {
            document.getElementById('api-processing-text').textContent = 'Step 1: POST Authentication...';
            document.getElementById('api-processing-subtext').textContent = 'Getting bearer token from authentication endpoint';
        } else {
            document.getElementById('api-processing-text').textContent = 'Connecting to API...';
            document.getElementById('api-processing-subtext').textContent = 'Fetching data';
        }

        const authConfig = buildAuthConfig();
        const additionalHeaders = {};

        if (headersText) {
            const headerLines = headersText.split('\n');
            for (const line of headerLines) {
                const [key, ...valueParts] = line.split(':');
                if (key && valueParts.length > 0) {
                    additionalHeaders[key.trim()] = valueParts.join(':').trim();
                }
            }
        }

        const allHeaders = Object.assign({}, authConfig.headers, additionalHeaders);

        const requestBody = {
            url: url,
            method: method,
            headers: allHeaders,
            username: authConfig.username,
            password: authConfig.password,
            authType: authConfig.authType,

            // NEW: POST authentication parameters
            requiresPostAuth: requiresPostAuth,
            dataUrl: dataFetchUrl || null
        };

        if (['POST', 'PUT', 'PATCH'].includes(method) && bodyText) {
            requestBody.body = bodyText;
        }

        const response = await fetch('/api/fetch-api-data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        const result = await response.json();

        if (!result.success) {
            throw new Error(result.error || 'API fetch failed');
        }

        console.log('âœ… API data fetched successfully');

        currentApiDataId = result.dataId;

        // Show success message
        if (requiresPostAuth) {
            document.getElementById('api-processing-text').textContent = 'âœ… Authentication & Data Fetch Complete!';
            document.getElementById('api-processing-subtext').textContent = `POST auth successful â†' Bearer token received â†' Data fetched: ${result.dataPreview.totalRecords} records`;
        } else {
            document.getElementById('api-processing-text').textContent = 'âœ… API Connection Successful!';
            document.getElementById('api-processing-subtext').textContent = `Loading ${result.dataPreview.totalRecords} records...`;
        }

        setTimeout(function() {
            displayApiPreview(result.dataPreview, result.metadata);
            document.getElementById('api-processing-section').style.display = 'none';
            document.getElementById('api-upload-section').style.display = 'block';

            const compareBtn = document.getElementById('startApiComparison');
            compareBtn.disabled = false;
        }, 1500);

    } catch (error) {
        console.error('API fetch failed:', error);

        document.getElementById('api-processing-section').style.display = 'none';
        document.getElementById('api-upload-section').style.display = 'block';

        alert('API fetch failed: ' + error.message);

    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
});

// FIXED: displayApiPreview function with proper authentication response handling
function displayApiPreview(preview, metadata) {
    try {
        console.log('🔍 DISPLAYING ENHANCED API PREVIEW WITH AUTHENTICATION DETECTION');
        console.log('Preview data received:', preview);
        console.log('Metadata received:', metadata);

        // Set API data source
        const dataSourceElement = document.getElementById('apiDataSource');
        if (dataSourceElement) {
            dataSourceElement.textContent = metadata?.url || 'API Endpoint';
        }

        // CRITICAL FIX: Proper bearer token detection from multiple sources
        const capturedBearerToken = preview.bearerToken ||
                                    preview.authToken ||
                                    preview.token ||
                                    metadata?.bearerToken ||
                                    metadata?.authToken ||
                                    null;

        // CRITICAL FIX: Better authentication response detection
        // Check if this is an auth response by looking at multiple indicators
        const hasToken = !!capturedBearerToken;
        const hasLowRecordCount = !preview.totalRecords || preview.totalRecords <= 1;
        const statusCode = metadata?.status || 200;
        const isAuthEndpoint = (metadata?.url || '').toLowerCase().includes('auth');

        // This is likely an auth response if:
        // 1. Has a bearer token, OR
        // 2. Has low/zero records AND is an auth endpoint AND got 200 status
        const isAuthenticationResponse = hasToken ||
                                        (hasLowRecordCount && isAuthEndpoint && statusCode === 200);

        console.log('🔍 Authentication Response Detection:');
        console.log('   - Has Bearer Token:', hasToken);
        console.log('   - Token Value:', capturedBearerToken ? 'Present' : 'None');
        console.log('   - Total Records:', preview.totalRecords || 0);
        console.log('   - Is Auth Endpoint:', isAuthEndpoint);
        console.log('   - Status Code:', statusCode);
        console.log('   - Is Auth Response:', isAuthenticationResponse);

        if (isAuthenticationResponse) {
            console.log('✅ DETECTED: This is an authentication response');

            // Display authentication success stats
            const apiStats = document.getElementById('api-stats');
            if (apiStats) {
                apiStats.innerHTML = `
                    <div class="api-stat-card" style="border-left: 4px solid #27ae60;">
                        <div class="api-stat-number" style="color: #27ae60; font-size: 2rem;">✅</div>
                        <div class="api-stat-label">Authentication Successful</div>
                        <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">Bearer token ${hasToken ? 'received' : 'generated'}</div>
                    </div>
                    <div class="api-stat-card" style="border-left: 4px solid #3498db;">
                        <div class="api-stat-number" style="color: #3498db; font-size: 1.3rem;">${statusCode}</div>
                        <div class="api-stat-label">HTTP Status</div>
                        <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">POST auth response</div>
                    </div>
                    <div class="api-stat-card" style="border-left: 4px solid #9b59b6;">
                        <div class="api-stat-number" style="color: #9b59b6;">${metadata?.duration || 0}ms</div>
                        <div class="api-stat-label">Response Time</div>
                        <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">Authentication latency</div>
                    </div>
                    <div class="api-stat-card" style="border-left: 4px solid #e67e22;">
                        <div class="api-stat-number" style="color: #e67e22;">🔑</div>
                        <div class="api-stat-label">Token Ready</div>
                        <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">Ready for data fetch</div>
                    </div>
                `;
            }

            // Show authentication-specific field suggestions
            const fieldSuggestions = document.getElementById('api-field-suggestions');
            if (fieldSuggestions) {
                fieldSuggestions.innerHTML = `
                    <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; border-left: 4px solid #27ae60;">
                        <h4 style="color: #2c3e50; margin-bottom: 12px;">🔐 POST Authentication Complete</h4>
                        <p style="color: #555; margin-bottom: 15px; line-height: 1.6;">
                            Your POST authentication was successful! ${hasToken ? 'A bearer token has been generated' : 'Authentication completed'} and is ready to use for data fetch requests.
                        </p>
                        ${hasToken ? `
                            <div style="background: #fff9e6; padding: 15px; border-radius: 6px; margin-bottom: 15px; border-left: 4px solid #f39c12;">
                                <p style="color: #666; margin: 0; font-size: 0.9rem;">
                                    <strong>Next Step:</strong> The bearer token will automatically be included in your data fetch request.
                                    Configure your data fetch URL and click "Fetch API Data" to proceed.
                                </p>
                            </div>
                        ` : ''}
                        <p style="color: #666; font-size: 0.85rem; margin: 0;">
                            ${hasToken ? 'Token type: <strong>Bearer Token (OAuth 2.0)</strong><br>' : ''}
                            Status: <strong style="color: #27ae60;">✅ Ready for Data Fetch</strong>
                        </p>
                    </div>
                `;
            }

            // CRITICAL: Display Bearer Token prominently if available
            if (hasToken) {
                displayBearerToken(capturedBearerToken, metadata?.url);
            }

            // Show the API preview section
            const apiPreview = document.getElementById('api-preview');
            if (apiPreview) {
                apiPreview.style.display = 'block';
            }

            console.log('✅ Authentication response preview displayed successfully');
            return; // Exit early - don't process as data response
        }

        // NORMAL DATA RESPONSE HANDLING (existing code)
        console.log('📊 Processing as normal data response...');

        // Use correct total counts from metadata
        const totalRecordsInAPI = metadata?.totalRecordsInAPI ||
                                  metadata?.totalRecordsAvailable ||
                                  preview.totalRecordsAvailable ||
                                  preview.totalRecords || 0;

        const recordsForComparison = metadata?.recordsForComparison ||
                                     preview.sampleSize ||
                                     preview.totalRecords || 0;

        const fieldsDetected = preview.fieldsDetected || 0;
        const isCompleteFetch = metadata?.isCompleteFetch || false;
        const strategy = metadata?.fetchStrategy || 'unknown';

        console.log(`✅ CORRECTED DISPLAY VALUES:`);
        console.log(`   - Total Records in API: ${totalRecordsInAPI}`);
        console.log(`   - Records for Comparison: ${recordsForComparison}`);
        console.log(`   - Strategy: ${strategy}`);
        console.log(`   - Is Complete: ${isCompleteFetch}`);

        // Enhanced API stats display with CORRECT values
        const apiStats = document.getElementById('api-stats');
        if (apiStats) {
            apiStats.innerHTML = `
                <div class="api-stat-card" style="border-left: 4px solid #3498db;">
                    <div class="api-stat-number" style="color: #3498db; font-size: 1.8rem;">${totalRecordsInAPI.toLocaleString()}</div>
                    <div class="api-stat-label">Total Records in API</div>
                    <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">${isCompleteFetch ? 'Complete dataset' : 'From API response'}</div>
                </div>
                <div class="api-stat-card" style="border-left: 4px solid #27ae60;">
                    <div class="api-stat-number" style="color: #27ae60; font-size: 1.8rem;">${recordsForComparison.toLocaleString()}</div>
                    <div class="api-stat-label">Records for Comparison</div>
                    <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">${isCompleteFetch ? 'Complete dataset' : `Sample (${((recordsForComparison / totalRecordsInAPI) * 100).toFixed(2)}%)`}</div>
                </div>
                <div class="api-stat-card" style="border-left: 4px solid #f39c12;">
                    <div class="api-stat-number" style="color: #f39c12;">${formatFileSize(preview.fileSize || 0)}</div>
                    <div class="api-stat-label">Response Size</div>
                    <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">JSON payload</div>
                </div>
                <div class="api-stat-card" style="border-left: 4px solid #9b59b6;">
                    <div class="api-stat-number" style="color: #9b59b6;">${fieldsDetected}</div>
                    <div class="api-stat-label">Fields Available</div>
                    <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">Schema columns</div>
                </div>
                <div class="api-stat-card" style="border-left: 4px solid #2ecc71;">
                    <div class="api-stat-number" style="color: #2ecc71; font-size: 1.2rem;">${metadata?.status || 200}</div>
                    <div class="api-stat-label">HTTP Status</div>
                    <div style="font-size: 0.75rem; color: #666; margin-top: 4px;">API response</div>
                </div>
            `;
        }

        // ROBUST FIELD EXTRACTION WITH MULTIPLE FALLBACKS
        let allFields = [];

        console.log('🔍 Attempting field extraction...');

        // Strategy 1: Use availableFields from preview
        if (preview.availableFields && Array.isArray(preview.availableFields) && preview.availableFields.length > 0) {
            allFields = preview.availableFields;
            console.log('✅ Strategy 1: Found availableFields:', allFields.length, 'fields');
        }
        // Strategy 2: Use allFieldsList from preview
        else if (preview.allFieldsList && Array.isArray(preview.allFieldsList) && preview.allFieldsList.length > 0) {
            allFields = preview.allFieldsList;
            console.log('✅ Strategy 2: Found allFieldsList:', allFields.length, 'fields');
        }
        // Strategy 3: Extract from sampleRecords
        else if (preview.sampleRecords && Array.isArray(preview.sampleRecords) && preview.sampleRecords.length > 0) {
            const sampleRecord = preview.sampleRecords[0];
            if (sampleRecord && typeof sampleRecord === 'object') {
                allFields = Object.keys(sampleRecord);
                console.log('✅ Strategy 3: Extracted from sampleRecords:', allFields.length, 'fields');
            }
        }
        // Strategy 4: Extract from originalSample
        else if (preview.originalSample && typeof preview.originalSample === 'object') {
            allFields = Object.keys(preview.originalSample);
            console.log('✅ Strategy 4: Extracted from originalSample:', allFields.length, 'fields');
        }

        console.log(`🔍 Final extracted fields (${allFields.length}):`, allFields);

        // Enhanced field suggestions display
        const fieldSuggestions = document.getElementById('api-field-suggestions');
        if (fieldSuggestions) {
            if (allFields.length === 0) {
                console.log('⚠️ No fields found - showing error message');
                fieldSuggestions.innerHTML = `
                    <div style="color: #f39c12; padding: 15px; background: #fff3cd; border-radius: 8px; border-left: 4px solid #f39c12;">
                        <h4 style="margin-bottom: 10px;">⚠️ Field Detection Issue</h4>
                        <p><strong>Status:</strong> ${preview.fieldsDetected || 0} fields detected but field list not available</p>
                        <p><strong>Solution:</strong> Manually type your primary key field name in the input box above</p>
                        <p><strong>For ServiceNow APIs, try:</strong> sys_id, u_number, number, serial_number</p>
                    </div>
                `;
            } else {
                console.log('✅ Displaying field suggestions for', allFields.length, 'fields');

                const idFields = preview.idFields || allFields.filter(field => {
                    const lowerField = field.toLowerCase();
                    return lowerField.includes('id') || lowerField.includes('key') || lowerField.includes('number');
                });

                const importantFields = preview.importantFields || allFields.filter(field => {
                    const lowerField = field.toLowerCase();
                    return !idFields.includes(field) && (
                        lowerField.includes('name') || lowerField.includes('status') ||
                        lowerField.includes('value') || lowerField.includes('type') ||
                        lowerField.includes('category')
                    );
                });

                let suggestionsHTML = '';

                if (idFields.length > 0) {
                    suggestionsHTML += `
                        <div class="field-category-header first">
                            🔑 ID/Key Fields (Recommended - Click to Use):
                        </div>
                        <div style="margin-bottom: 15px;">
                    `;
                    idFields.forEach(field => {
                        suggestionsHTML += `<span class="clickable-field" onclick="setAsApiPrimaryKey('${field}')" title="Click to use ${field} as primary key">${field}</span> `;
                    });
                    suggestionsHTML += '</div>';
                }

                if (importantFields.length > 0) {
                    suggestionsHTML += `
                        <div class="field-category-header">
                            📋 Other Important Fields (Click to Use):
                        </div>
                        <div>
                    `;
                    importantFields.forEach(field => {
                        suggestionsHTML += `<span class="clickable-field" onclick="setAsApiPrimaryKey('${field}')" title="Click to use ${field} as primary key">${field}</span> `;
                    });
                    suggestionsHTML += '</div>';
                }

                // Show all fields if no categorization worked
                if (suggestionsHTML === '') {
                    suggestionsHTML = `
                        <div class="field-category-header first">
                            📋 All Available Fields (Click to Use):
                        </div>
                        <div>
                    `;
                    allFields.forEach(field => {
                        suggestionsHTML += `<span class="clickable-field" onclick="setAsApiPrimaryKey('${field}')" title="Click to use ${field} as primary key">${field}</span> `;
                    });
                    suggestionsHTML += '</div>';
                }

                fieldSuggestions.innerHTML = suggestionsHTML;
            }
        }

        // Show the API preview section
        const apiPreview = document.getElementById('api-preview');
        if (apiPreview) {
            apiPreview.style.display = 'block';
        }

        console.log('✅ API preview displayed successfully with CORRECT total records');

    } catch (error) {
        console.error('❌ Error in displayApiPreview:', error);

        const fieldSuggestions = document.getElementById('api-field-suggestions');
        if (fieldSuggestions) {
            fieldSuggestions.innerHTML = `
                <div style="color: #e74c3c; padding: 15px; background: #fef2f2; border-radius: 8px; border-left: 4px solid #e74c3c;">
                    <h4>❌ Preview Error</h4>
                    <p>Error: ${error.message}</p>
                    <p>You can still manually type the primary key field name above and proceed with comparison.</p>
                </div>
            `;
        }

        alert('API data fetched successfully but preview display failed. You can still proceed with comparison.');
    }
}

// NEW: Helper function to display bearer token prominently
function displayBearerToken(token, apiUrl) {
    const apiPreview = document.getElementById('api-preview');
    if (!apiPreview) return;

    // Remove any existing token display
    const existingTokenDisplay = apiPreview.querySelector('.bearer-token-display');
    if (existingTokenDisplay) {
        existingTokenDisplay.remove();
    }

    const tokenSection = document.createElement('div');
    tokenSection.className = 'bearer-token-display';
    tokenSection.style.cssText = `
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 25px;
        border-radius: 12px;
        margin-top: 20px;
        border-left: 5px solid #764ba2;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.2);
    `;

    const tokenDisplay = `
        <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 20px;">
            <div style="flex: 1;">
                <h4 style="margin: 0 0 12px 0; font-size: 16px; font-weight: 600; display: flex; align-items: center; gap: 8px;">
                    🔐 Bearer Token Generated Successfully
                </h4>
                <p style="margin: 0 0 12px 0; font-size: 13px; opacity: 0.95; line-height: 1.5;">
                    Your authentication was successful. The bearer token below will be automatically used for all subsequent API data fetch requests.
                </p>
                <div style="background: rgba(0,0,0,0.25); padding: 12px; border-radius: 8px; font-family: 'Courier New', monospace; font-size: 12px; word-break: break-all; max-height: 80px; overflow-y: auto;">
                    <span style="color: #e0e0e0;">Bearer ${token}</span>
                </div>
            </div>
            <button
                onclick="navigator.clipboard.writeText('Bearer ${token}').then(() => alert('Bearer token copied to clipboard!')).catch(err => console.error('Failed to copy:', err));"
                style="
                    background: white;
                    color: #667eea;
                    border: none;
                    padding: 12px 20px;
                    border-radius: 8px;
                    cursor: pointer;
                    font-weight: 600;
                    font-size: 13px;
                    white-space: nowrap;
                    flex-shrink: 0;
                    transition: all 0.3s ease;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                "
                onmouseover="this.style.background='#f5f5f5'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.2)';"
                onmouseout="this.style.background='white'; this.style.boxShadow='0 2px 8px rgba(0,0,0,0.15)';"
            >
                📋 Copy Token
            </button>
        </div>
        <div style="margin-top: 15px; padding: 12px; background: rgba(255,255,255,0.15); border-radius: 6px; font-size: 12px;">
            <strong>API URL:</strong> <span style="opacity: 0.9;">${apiUrl || 'N/A'}</span><br>
            <strong>Status:</strong> <span style="color: #4caf50; font-weight: 600;">✅ Ready to Fetch Data</span>
        </div>
    `;

    tokenSection.innerHTML = tokenDisplay;
    apiPreview.appendChild(tokenSection);

    console.log('✅ Bearer token display added to preview');
}
        // ========================================
        // ENHANCED API PREVIEW DISPLAY
        // ========================================

        // Enhanced API Comparison
        // Replace the existing startApiComparison event listener in index.html
// with this enhanced version that includes BigQuery filtering

document.getElementById('startApiComparison').addEventListener('click', async function() {
    if (!currentApiDataId) {
        alert('Please fetch API data first!');
        return;
    }

    const primaryKey = document.getElementById('apiPrimaryKey').value.trim();
    const sourceTable = document.getElementById('apiBqTable').value.trim();
    const bqFilter = document.getElementById('apiBqFilter').value.trim(); // NEW: Get filter condition

    if (!sourceTable) {
        alert('Please enter a BigQuery table name!');
        return;
    }

    if (!primaryKey) {
        alert('Please enter a primary key field name!');
        return;
    }

    const button = this;
    const originalText = button.innerHTML;

    try {
        button.disabled = true;
        button.innerHTML = '🔄 Running Comprehensive API vs BQ Analysis with Optional Filtering...';

        document.getElementById('api-upload-section').style.display = 'none';
        document.getElementById('api-processing-section').style.display = 'block';

        // Enhanced processing text with filter information
        if (bqFilter) {
            document.getElementById('api-processing-text').textContent = 'Creating BigQuery temp table from API data with filtering...';
            document.getElementById('api-processing-subtext').textContent = `Processing API response with BigQuery filter: ${bqFilter.substring(0, 100)}${bqFilter.length > 100 ? '...' : ''}`;
        } else {
            document.getElementById('api-processing-text').textContent = 'Creating BigQuery temp table from API data...';
            document.getElementById('api-processing-subtext').textContent = 'Processing API response with authentication validation';
        }

        // Step 1: Create temp table from API data
        const tempResponse = await fetch('/api/create-temp-table-from-api', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dataId: currentApiDataId,
                primaryKey: primaryKey
            })
        });

        const tempResult = await tempResponse.json();

        if (!tempResult.success) {
            throw new Error(tempResult.error || 'Failed to create temp table from API data');
        }

        console.log('✅ API temp table created successfully');

        // Enhanced processing text for comparison phase
        if (bqFilter) {
            document.getElementById('api-processing-text').textContent = 'Running comprehensive API vs BQ comparison with BigQuery filtering...';
            document.getElementById('api-processing-subtext').textContent = 'Applying filter condition to BigQuery data and analyzing schema, records, fields, and data quality';
        } else {
            document.getElementById('api-processing-text').textContent = 'Running comprehensive API vs BQ comparison...';
            document.getElementById('api-processing-subtext').textContent = 'Analyzing schema, records, fields, and data quality across systems';
        }

        // NEW: Get explode array field value
        const explodeArrayField = document.getElementById('apiExplodeArrayField').value.trim();
        
        // Update processing text if array explosion is enabled
        if (explodeArrayField) {
            document.getElementById('api-processing-subtext').textContent += ` | Exploding nested array: ${explodeArrayField}`;
        }

        // Step 2: Run comprehensive comparison with optional BigQuery filtering
        const compResponse = await fetch('/api/compare-api-vs-bq-comprehensive', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dataId: currentApiDataId,
                sourceTable: sourceTable,
                primaryKey: primaryKey,
                comparisonFields: [], // Will analyze all common fields
                includeFieldAnalysis: true,
                includeDuplicateAnalysis: true,
                includeSchemaAnalysis: true,
                bqFilter: bqFilter, // Include BigQuery filter condition
                explodeArrayField: explodeArrayField // NEW: Include explode array field
            })
        });

        const compResult = await compResponse.json();

        if (!compResult.success) {
            throw new Error(compResult.error || 'API comprehensive comparison failed');
        }

        console.log('✅ API comprehensive comparison with optional filtering completed');

        displayApiComparisonResults(compResult);

        document.getElementById('api-processing-section').style.display = 'none';
        document.getElementById('api-comparison-results').style.display = 'block';

    } catch (error) {
        console.error('API comparison failed:', error);

        document.getElementById('api-processing-section').style.display = 'none';
        document.getElementById('api-upload-section').style.display = 'block';

        let errorMsg = error.message;

        // Enhanced error handling for filter-related issues
        if (error.message.includes('Invalid filter condition')) {
            errorMsg += '\n\n🔍 Filter Syntax Help:\n• Use standard SQL WHERE clause syntax\n• Example: account_id = \'your-account-id\'\n• Example: status IN (\'active\', \'enabled\')\n• Example: created_date >= \'2024-01-01\'';
        } else if (error.message.includes('Filter field not found')) {
            errorMsg += '\n\n📝 Filter Field Tips:\n• Check field names exist in your BigQuery table\n• Field names are case-sensitive\n• Use the Column Names tab to see available fields';
        } else if (error.message.includes('authentication')) {
            errorMsg += '\n\n🔐 Authentication issue detected. Please verify your API credentials and test the connection first.';
        } else if (error.message.includes('not available in both tables')) {
            errorMsg += '\n\nTip: Try using a field name that exists in both your API data and BigQuery table. Check the field suggestions above.';
        } else if (error.message.includes('403') || error.message.includes('Forbidden')) {
            errorMsg += '\n\n🚫 Access denied. Your API credentials may not have permission to access this data.';
        }

        alert('API vs BQ comparison failed: ' + errorMsg);
    } finally {
        button.disabled = false;
        button.innerHTML = originalText;
    }
});

// Enhanced function to reset API comparison form (includes filter reset)
function startNewApiComparison() {
    // Reset dependency states for API section
    connectionTestPassed = false;
    apiDataFetched = false;
    disableFetchDataButton();

    document.getElementById('api-comparison-results').style.display = 'none';
    document.getElementById('api-upload-section').style.display = 'block';
    document.getElementById('api-processing-section').style.display = 'none';
    document.getElementById('startApiComparison').disabled = true;
    document.getElementById('api-preview').style.display = 'none';

    // Clear all form fields including the new filter field
    document.getElementById('apiUrl').value = '';
    document.getElementById('apiUsername').value = '';
    document.getElementById('apiPassword').value = '';
    document.getElementById('apiKeyHeader').value = 'X-API-Key';
    document.getElementById('apiKeyValue').value = '';
    document.getElementById('bearerToken').value = '';
    document.getElementById('apiHeaders').value = '';
    document.getElementById('apiBody').value = '';
    document.getElementById('apiPrimaryKey').value = '';
    document.getElementById('apiBqTable').value = '';
    document.getElementById('apiBqFilter').value = ''; // Clear BigQuery filter
    document.getElementById('apiExplodeArrayField').value = ''; // NEW: Clear explode array field

    // Reset authentication type
    document.getElementById('authType').value = 'none';
    toggleAuthFields();

    // Hide connection test result
    document.getElementById('connectionTestResult').style.display = 'none';

    // Reset method to GET
    document.querySelectorAll('.method-option').forEach(opt => opt.classList.remove('selected'));
    document.querySelector('.method-option[data-method="GET"]').classList.add('selected');
    document.getElementById('apiBodySection').style.display = 'none';

    currentApiDataId = null;
    globalApiResults = null;

    const exportButton = document.getElementById('exportApiToExcel');
    if (exportButton) exportButton.disabled = true;

    console.log('API interface reset completely with BigQuery filter and explode array field clearing');
}

// ========== MULTI-API BATCH FUNCTIONALITY ==========

// Show API tab (single or multi)
function showAPITab(tabType) {
    // Hide all tabs
    document.getElementById('single-api-tab').style.display = 'none';
    document.getElementById('multi-api-tab').style.display = 'none';
    
    // Remove active class from all buttons
    document.querySelectorAll('#api-comparison-page .results-tab-button').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab and activate button
    if (tabType === 'single') {
        document.getElementById('single-api-tab').style.display = 'block';
        document.querySelector('#api-comparison-page .results-tab-button:first-child').classList.add('active');
    } else {
        document.getElementById('multi-api-tab').style.display = 'block';
        document.querySelector('#api-comparison-page .results-tab-button:last-child').classList.add('active');
    }
}

// Toggle multi-API auth fields
function toggleMultiApiAuthFields() {
    const authType = document.getElementById('multi-api-auth-type').value;
    
    document.getElementById('multi-api-cloudflare-fields').style.display = 'none';
    document.getElementById('multi-api-bearer-fields').style.display = 'none';
    document.getElementById('multi-api-apikey-fields').style.display = 'none';
    document.getElementById('multi-api-basic-fields').style.display = 'none';
    
    if (authType === 'cloudflare') {
        document.getElementById('multi-api-cloudflare-fields').style.display = 'block';
    } else if (authType === 'bearer') {
        document.getElementById('multi-api-bearer-fields').style.display = 'block';
    } else if (authType === 'apikey') {
        document.getElementById('multi-api-apikey-fields').style.display = 'block';
    } else if (authType === 'basic') {
        document.getElementById('multi-api-basic-fields').style.display = 'block';
    }
}

// Test multi-API connection
async function testMultiApiConnection() {
    const statusDiv = document.getElementById('multi-api-connection-status');
    const baseUrl = document.getElementById('multi-api-base-url').value.trim();
    const endpointsRaw = document.getElementById('multi-api-endpoints').value.trim();
    const accountId = document.getElementById('multi-api-account-id').value.trim();
    const zoneId = document.getElementById('multi-api-zone-id').value.trim();
    
    if (!baseUrl || !endpointsRaw) {
        statusDiv.innerHTML = '<div style="color: #721c24; background: #f8d7da; padding: 10px; border-radius: 4px;">Please enter base URL and at least one endpoint</div>';
        return;
    }
    
    const endpoints = endpointsRaw.split('\n').map(e => e.trim()).filter(e => e);
    if (endpoints.length === 0) {
        statusDiv.innerHTML = '<div style="color: #721c24; background: #f8d7da; padding: 10px; border-radius: 4px;">Please enter at least one endpoint</div>';
        return;
    }
    
    // Build first endpoint URL for testing
    let testEndpoint = endpoints[0]
        .replace('{account_id}', accountId)
        .replace('{zone_id}', zoneId);
    
    const testUrl = baseUrl + testEndpoint;
    
    // Build headers based on auth type
    const authType = document.getElementById('multi-api-auth-type').value;
    const headers = { 'Content-Type': 'application/json' };
    
    if (authType === 'cloudflare') {
        const email = document.getElementById('multi-api-cf-email').value.trim();
        const key = document.getElementById('multi-api-cf-key').value.trim();
        if (email) headers['X-Auth-Email'] = email;
        if (key) headers['X-Auth-Key'] = key;
    } else if (authType === 'bearer') {
        const token = document.getElementById('multi-api-bearer-token').value.trim();
        if (token) headers['Authorization'] = `Bearer ${token}`;
    } else if (authType === 'apikey') {
        const keyHeader = document.getElementById('multi-api-key-header').value.trim() || 'X-Auth-Key';
        const keyValue = document.getElementById('multi-api-key-value').value.trim();
        const email = document.getElementById('multi-api-email').value.trim();
        if (keyValue) headers[keyHeader] = keyValue;
        if (email) headers['X-Auth-Email'] = email;
    } else if (authType === 'basic') {
        const username = document.getElementById('multi-api-username').value.trim();
        const password = document.getElementById('multi-api-password').value.trim();
        if (username && password) {
            headers['Authorization'] = 'Basic ' + btoa(username + ':' + password);
        }
    }
    
    statusDiv.innerHTML = '<div style="color: #856404; background: #fff3cd; padding: 10px; border-radius: 4px;">🔄 Testing connection...</div>';
    
    try {
        const response = await fetch('/api/proxy-api-request', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: testUrl,
                method: 'GET',
                headers: headers
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            statusDiv.innerHTML = `
                <div style="color: #155724; background: #d4edda; padding: 10px; border-radius: 4px;">
                    ✅ Connection successful!<br>
                    <small>Tested: ${testUrl}</small>
                </div>
            `;
        } else {
            statusDiv.innerHTML = `
                <div style="color: #721c24; background: #f8d7da; padding: 10px; border-radius: 4px;">
                    ❌ Connection failed: ${result.error || 'Unknown error'}<br>
                    <small>URL: ${testUrl}</small>
                </div>
            `;
        }
    } catch (error) {
        statusDiv.innerHTML = `
            <div style="color: #721c24; background: #f8d7da; padding: 10px; border-radius: 4px;">
                ❌ Connection error: ${error.message}
            </div>
        `;
    }
}

// Start multi-API comparison
async function startMultiApiComparison() {
    const configSection = document.getElementById('multi-api-config-section');
    const resultsSection = document.getElementById('multi-api-results');
    
    // Get configuration
    const baseUrl = document.getElementById('multi-api-base-url').value.trim();
    const endpointsRaw = document.getElementById('multi-api-endpoints').value.trim();
    const accountId = document.getElementById('multi-api-account-id').value.trim();
    const zoneId = document.getElementById('multi-api-zone-id').value.trim();
    const bqDataset = document.getElementById('multi-api-bq-dataset').value.trim();
    const bqTablesRaw = document.getElementById('multi-api-bq-tables').value.trim();
    const primaryKeysRaw = document.getElementById('multi-api-primary-keys').value.trim();
    const bqFiltersRaw = document.getElementById('multi-api-bq-filters').value.trim();
    const dataPath = document.getElementById('multi-api-data-path').value.trim() || 'result';
    
    const endpoints = endpointsRaw.split('\n').map(e => e.trim()).filter(e => e);
    const bqTables = bqTablesRaw.split('\n').map(t => t.trim()).filter(t => t);
    const primaryKeys = primaryKeysRaw.split('\n').map(k => k.trim()).filter(k => k);
    // Parse BQ filters - allow empty lines for tables without filters
    const bqFilters = bqFiltersRaw.split('\n').map(f => f.trim());
    
    // Validation
    if (!baseUrl || endpoints.length === 0 || !bqDataset || bqTables.length === 0 || primaryKeys.length === 0) {
        alert('Please fill in all required fields: Base URL, Endpoints, BQ Dataset, BQ Tables, and Primary Keys');
        return;
    }
    
    if (endpoints.length !== bqTables.length) {
        alert(`Mismatch: ${endpoints.length} endpoints but ${bqTables.length} BQ tables. They must match 1:1.`);
        return;
    }
    
    // Warn if primary keys don't match but allow proceeding
    if (primaryKeys.length !== endpoints.length && primaryKeys.length !== 1) {
        const proceed = confirm(`You have ${endpoints.length} endpoints but ${primaryKeys.length} primary keys.\n\nIf you have 1 primary key, it will be used for all tables.\nOtherwise, primary keys should match endpoints 1:1.\n\nDo you want to proceed anyway?`);
        if (!proceed) return;
    }
    
    // Build auth headers
    const authType = document.getElementById('multi-api-auth-type').value;
    const headers = { 'Content-Type': 'application/json' };
    
    if (authType === 'cloudflare') {
        const email = document.getElementById('multi-api-cf-email').value.trim();
        const key = document.getElementById('multi-api-cf-key').value.trim();
        if (email) headers['X-Auth-Email'] = email;
        if (key) headers['X-Auth-Key'] = key;
    } else if (authType === 'bearer') {
        const token = document.getElementById('multi-api-bearer-token').value.trim();
        if (token) headers['Authorization'] = `Bearer ${token}`;
    } else if (authType === 'apikey') {
        const keyHeader = document.getElementById('multi-api-key-header').value.trim() || 'X-Auth-Key';
        const keyValue = document.getElementById('multi-api-key-value').value.trim();
        const email = document.getElementById('multi-api-email').value.trim();
        if (keyValue) headers[keyHeader] = keyValue;
        if (email) headers['X-Auth-Email'] = email;
    } else if (authType === 'basic') {
        const username = document.getElementById('multi-api-username').value.trim();
        const password = document.getElementById('multi-api-password').value.trim();
        if (username && password) {
            headers['Authorization'] = 'Basic ' + btoa(username + ':' + password);
        }
    }
    
    // Hide config, show results
    configSection.style.display = 'none';
    resultsSection.style.display = 'block';
    
    // Process each API endpoint
    const allResults = [];
    const totalApis = endpoints.length;
    
    for (let i = 0; i < totalApis; i++) {
        const endpoint = endpoints[i]
            .replace('{account_id}', accountId)
            .replace('{zone_id}', zoneId);
        const apiUrl = baseUrl + endpoint;
        const bqTable = `${bqDataset}.${bqTables[i]}`;
        const primaryKey = primaryKeys[i] || primaryKeys[0];
        const bqFilter = bqFilters[i] || ''; // Get filter for this table (empty string if not specified)
        
        // Show progress
        resultsSection.innerHTML = `
            <div style="text-align: center; padding: 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px; color: white; margin: 20px;">
                <div class="spinner" style="margin: 0 auto 20px; width: 50px; height: 50px; border: 4px solid rgba(255,255,255,0.3); border-top: 4px solid white; border-radius: 50%; animation: spin 1s linear infinite;"></div>
                <div style="font-size: 22px; font-weight: bold; margin-bottom: 10px;">🔄 Processing API ${i + 1} of ${totalApis}...</div>
                <div style="font-size: 14px; opacity: 0.9; margin-bottom: 10px;">${endpoint}</div>
                <div style="font-size: 13px; opacity: 0.8;">→ ${bqTable}</div>
                ${bqFilter ? `<div style="font-size: 12px; opacity: 0.7; margin-top: 5px;">🔍 Filter: ${bqFilter}</div>` : ''}
                <div style="background: rgba(255,255,255,0.3); border-radius: 10px; height: 24px; overflow: hidden; margin-top: 20px;">
                    <div style="background: rgba(255,255,255,0.9); height: 100%; width: ${((i + 1) / totalApis) * 100}%; transition: width 0.5s;"></div>
                </div>
            </div>
        `;
        
        try {
            // Step 1: Fetch API data
            console.log(`\n=== Processing API ${i + 1}/${totalApis} ===`);
            console.log(`Endpoint: ${endpoint}`);
            console.log(`Full URL: ${apiUrl}`);
            console.log(`Target BQ Table: ${bqTable}`);
            console.log(`Primary Key: ${primaryKey}`);
            console.log(`BQ Filter: ${bqFilter || '(none)'}`);
            
            const apiResponse = await fetch('/api/proxy-api-request', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    url: apiUrl,
                    method: 'GET',
                    headers: headers
                })
            });
            
            const apiResult = await apiResponse.json();
            console.log(`API Response success: ${apiResult.success}`);
            
            if (!apiResult.success) {
                console.error(`API request failed: ${apiResult.error}`);
                allResults.push({
                    endpoint: endpoint,
                    bqTable: bqTable,
                    success: false,
                    error: apiResult.error || 'API request failed'
                });
                continue;
            }
            
            // Extract data from response using dataPath
            let apiData = apiResult.data;
            if (dataPath && apiData[dataPath]) {
                apiData = apiData[dataPath];
                console.log(`Extracted data from path '${dataPath}': ${apiData.length} records`);
            }
            
            if (!Array.isArray(apiData)) {
                apiData = [apiData];
                console.log('Converted single object to array');
            }
            
            // Inject account_id and zone_id into each record if they don't exist
            // This allows using account_id/zone_id as primary keys even when API doesn't return them
            if (accountId || zoneId) {
                apiData = apiData.map(record => {
                    const enrichedRecord = { ...record };
                    if (accountId && !enrichedRecord.account_id) {
                        enrichedRecord.account_id = accountId;
                    }
                    if (zoneId && !enrichedRecord.zone_id) {
                        enrichedRecord.zone_id = zoneId;
                    }
                    return enrichedRecord;
                });
                console.log(`Injected account_id/zone_id into ${apiData.length} records`);
            }
            
            console.log(`API data records: ${apiData.length}`);
            
            // Step 2: Create temp table and compare
            console.log('Starting comparison with BigQuery...');
            console.log(`Using BQ filter: ${bqFilter || '(none)'}`);
            const compareResponse = await fetch('/api/api-vs-bq-compare', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    apiData: apiData,
                    bqTable: bqTable,
                    primaryKey: primaryKey,
                    bqFilter: bqFilter || null
                })
            });
            
            const compareResult = await compareResponse.json();
            console.log(`Comparison result success: ${compareResult.success}`);
            
            if (compareResult.success) {
                console.log(`Match rate: ${compareResult.summary?.pipelineSuccessRate || 'N/A'}%`);
            } else {
                console.error(`Comparison failed: ${compareResult.error}`);
            }
            
            allResults.push({
                endpoint: endpoint,
                bqTable: bqTable,
                bqFilter: bqFilter || null,
                success: compareResult.success,
                data: compareResult.success ? compareResult : null,
                error: compareResult.success ? null : compareResult.error,
                apiRecords: apiData.length
            });
            
        } catch (error) {
            console.error(`Error processing API ${i + 1}:`, error);
            allResults.push({
                endpoint: endpoint,
                bqTable: bqTable,
                success: false,
                error: error.message
            });
        }
    }
    
    // Display results
    displayMultiApiResults(allResults, resultsSection);
}

// Display multi-API results
let globalMultiApiResults = null; // Store results globally for detailed view

function displayMultiApiResults(results, container) {
    // Store results globally
    globalMultiApiResults = results;
    
    const successCount = results.filter(r => r.success).length;
    const failCount = results.length - successCount;
    const totalApiRecords = results.reduce((sum, r) => sum + (r.apiRecords || 0), 0);
    const totalMatches = results.reduce((sum, r) => {
        if (r.success && r.data) {
            const data = r.data.data || r.data;
            return sum + (data.summary?.identicalRecords || data.summary?.recordsReachedTarget || 0);
        }
        return sum;
    }, 0);
    
    // Build tabs
    const tabsHtml = results.map((r, idx) => {
        const tableName = r.bqTable.split('.').pop();
        const statusIcon = r.success ? '✅' : '❌';
        const activeClass = idx === 0 ? 'active' : '';
        return `<button class="multi-table-tab ${activeClass}" onclick="showMultiApiTab(${idx})" data-tab="${idx}">
            ${statusIcon} ${tableName}
        </button>`;
    }).join('');
    
    // Build content for each API
    const contentsHtml = results.map((r, idx) => {
        const displayStyle = idx === 0 ? 'block' : 'none';
        const tableName = r.bqTable.split('.').pop();
        
        if (r.success) {
            const data = r.data?.data || r.data || {};
            const summary = data.summary || {};
            const matchRate = summary.pipelineSuccessRate || summary.matchRate || '100';
            const apiRecords = r.apiRecords || 0;
            const matches = summary.identicalRecords || summary.recordsReachedTarget || 0;
            const mismatches = summary.mismatchedRecords || summary.recordsFailedToReachTarget || 0;
            const compResults = r.data?.comparisonResults || {};
            const apiOnly = compResults.missing?.missingFromBQ?.count || summary.recordsFailedToReachTarget || 0;
            const bqOnly = compResults.missing?.missingFromJSON?.count || summary.recordsOnlyInTarget || 0;
            const bqRecords = r.data?.recordCounts?.bqDetails?.totalRecords || r.data?.summary?.targetRecords || 0;
            const filterUsed = r.bqFilter || r.data?.metadata?.bqFilter || null;
            
            return `
                <div class="multi-api-content" id="multi-api-content-${idx}" style="display: ${displayStyle};">
                    <div style="background: #d4edda; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #28a745;">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                            <div>
                                <h4 style="margin: 0 0 10px 0; color: #155724;">✅ API ${idx + 1}: ${tableName}</h4>
                                <div style="color: #666; font-size: 13px;">
                                    <strong>Endpoint:</strong> ${r.endpoint}<br>
                                    <strong>Target:</strong> ${r.bqTable}
                                    ${filterUsed ? `<br><strong style="color: #e67e22;">🔍 Filter:</strong> <code style="background: #fef9e7; padding: 2px 6px; border-radius: 3px;">${filterUsed}</code>` : ''}
                                </div>
                            </div>
                            <button onclick="viewMultiApiDetailedResults(${idx})" 
                                    style="padding: 10px 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">
                                🔍 View Detailed Analysis
                            </button>
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 20px;">
                            <div style="background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <div style="font-size: 28px; font-weight: bold; color: #007bff;">${apiRecords.toLocaleString()}</div>
                                <div style="font-size: 12px; color: #666;">API Records</div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <div style="font-size: 28px; font-weight: bold; color: #6f42c1;">${bqRecords.toLocaleString()}</div>
                                <div style="font-size: 12px; color: #666;">BQ Records</div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <div style="font-size: 28px; font-weight: bold; color: #28a745;">${matches.toLocaleString()}</div>
                                <div style="font-size: 12px; color: #666;">Matches</div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <div style="font-size: 28px; font-weight: bold; color: #dc3545;">${mismatches.toLocaleString()}</div>
                                <div style="font-size: 12px; color: #666;">Mismatches</div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <div style="font-size: 28px; font-weight: bold; color: #17a2b8;">${matchRate}%</div>
                                <div style="font-size: 12px; color: #666;">Success Rate</div>
                            </div>
                        </div>
                        ${(apiOnly > 0 || bqOnly > 0) ? `
                        <div style="background: #fff3cd; padding: 15px; border-radius: 6px; border-left: 4px solid #ffc107;">
                            <h5 style="margin: 0 0 10px 0; color: #856404;">⚠️ Missing Records</h5>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                                <div style="background: white; padding: 10px; border-radius: 4px;">
                                    <span style="color: #e74c3c; font-weight: bold;">${apiOnly}</span> records in API only (not in BQ)
                                </div>
                                <div style="background: white; padding: 10px; border-radius: 4px;">
                                    <span style="color: #3498db; font-weight: bold;">${bqOnly}</span> records in BQ only (not in API)
                                </div>
                            </div>
                        </div>
                        ` : `
                        <div style="background: #d4edda; padding: 10px; border-radius: 6px; text-align: center;">
                            <span style="color: #155724;">✅ All records matched between API and BigQuery</span>
                        </div>
                        `}
                    </div>
                </div>
            `;
        } else {
            return `
                <div class="multi-api-content" id="multi-api-content-${idx}" style="display: ${displayStyle};">
                    <div style="background: #f8d7da; padding: 20px; border-radius: 8px; border-left: 4px solid #dc3545;">
                        <h4 style="margin: 0 0 10px 0; color: #721c24;">❌ API ${idx + 1}: ${tableName}</h4>
                        <div style="color: #666; margin-bottom: 10px; font-size: 13px;">
                            <strong>Endpoint:</strong> ${r.endpoint}<br>
                            <strong>Target:</strong> ${r.bqTable}
                        </div>
                        <p style="color: #721c24; margin: 10px 0;">${r.error}</p>
                    </div>
                </div>
            `;
        }
    }).join('');
    
    container.innerHTML = `
        <div style="padding: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h3 style="color: #495057; margin: 0;">📊 Multi-API vs BigQuery Validation Results</h3>
                <div style="display: flex; gap: 10px;">
                    <button onclick="exportMultiApiResultsToExcel()" style="padding: 10px 20px; background: linear-gradient(135deg, #28a745 0%, #20c997 100%); color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">
                        📥 Export to Excel
                    </button>
                    <button onclick="startNewMultiApiComparison()" style="padding: 10px 20px; background: #6c757d; color: white; border: none; border-radius: 6px; cursor: pointer;">
                        🔄 New Comparison
                    </button>
                </div>
            </div>
            
            <!-- Summary Cards -->
            <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 25px;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 32px; font-weight: bold;">${results.length}</div>
                    <div style="font-size: 13px; opacity: 0.9;">Total APIs</div>
                </div>
                <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 32px; font-weight: bold;">${successCount}</div>
                    <div style="font-size: 13px; opacity: 0.9;">Successful</div>
                </div>
                <div style="background: ${failCount > 0 ? 'linear-gradient(135deg, #eb3349 0%, #f45c43 100%)' : 'linear-gradient(135deg, #bdc3c7 0%, #2c3e50 100%)'}; padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 32px; font-weight: bold;">${failCount}</div>
                    <div style="font-size: 13px; opacity: 0.9;">Failed</div>
                </div>
                <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 32px; font-weight: bold;">${totalApiRecords.toLocaleString()}</div>
                    <div style="font-size: 13px; opacity: 0.9;">Total API Records</div>
                </div>
                <div style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 32px; font-weight: bold;">${totalMatches.toLocaleString()}</div>
                    <div style="font-size: 13px; opacity: 0.9;">Total Matches</div>
                </div>
            </div>
            
            <!-- Tabs -->
            <div style="border-bottom: 2px solid #dee2e6; margin-bottom: 20px;">
                <div style="display: flex; gap: 5px; flex-wrap: wrap;">
                    ${tabsHtml}
                </div>
            </div>
            
            <!-- Content -->
            ${contentsHtml}
        </div>
    `;
}

// Show multi-API tab
function showMultiApiTab(idx) {
    document.querySelectorAll('.multi-api-content').forEach(c => c.style.display = 'none');
    document.querySelectorAll('.multi-table-tab').forEach(t => t.classList.remove('active'));
    
    const content = document.getElementById(`multi-api-content-${idx}`);
    if (content) content.style.display = 'block';
    
    const tab = document.querySelector(`.multi-table-tab[data-tab="${idx}"]`);
    if (tab) tab.classList.add('active');
}

// Start new multi-API comparison
function startNewMultiApiComparison() {
    document.getElementById('multi-api-config-section').style.display = 'block';
    document.getElementById('multi-api-results').style.display = 'none';
    document.getElementById('multi-api-results').innerHTML = '';
    globalMultiApiResults = null;
    window.scrollTo({top: 0, behavior: 'smooth'});
}

// Export Multi-API results to Excel
function exportMultiApiResultsToExcel() {
    if (!globalMultiApiResults || globalMultiApiResults.length === 0) {
        alert('No Multi-API results available for export. Please run a comparison first.');
        return;
    }
    
    console.log('Starting Multi-API Excel export...');
    
    const wb = XLSX.utils.book_new();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T');
    
    // Calculate totals
    const successCount = globalMultiApiResults.filter(r => r.success).length;
    const failCount = globalMultiApiResults.length - successCount;
    const totalApiRecords = globalMultiApiResults.reduce((sum, r) => sum + (r.apiRecords || 0), 0);
    const totalMatches = globalMultiApiResults.reduce((sum, r) => {
        if (r.success && r.data) {
            const summary = r.data.summary || {};
            return sum + (summary.recordsReachedTarget || 0);
        }
        return sum;
    }, 0);
    
    // Sheet 1: Executive Summary
    const summaryData = [
        ['MULTI-API vs BIGQUERY VALIDATION REPORT'],
        [''],
        ['Report Generated:', new Date().toLocaleString()],
        [''],
        ['OVERALL SUMMARY'],
        ['Total APIs Validated', globalMultiApiResults.length],
        ['Successful Validations', successCount],
        ['Failed Validations', failCount],
        ['Total API Records', totalApiRecords],
        ['Total Matched Records', totalMatches],
        ['Overall Success Rate', totalApiRecords > 0 ? ((totalMatches / totalApiRecords) * 100).toFixed(1) + '%' : 'N/A'],
        [''],
        ['API VALIDATION DETAILS'],
        ['#', 'API Endpoint', 'Target Table', 'Filter', 'Status', 'API Records', 'BQ Records', 'Matches', 'Mismatches', 'Success Rate']
    ];
    
    globalMultiApiResults.forEach((r, idx) => {
        const tableName = r.bqTable.split('.').pop();
        if (r.success) {
            const summary = r.data?.summary || {};
            const recordCounts = r.data?.recordCounts || {};
            summaryData.push([
                idx + 1,
                r.endpoint,
                tableName,
                r.bqFilter || 'None',
                '✓ Success',
                r.apiRecords || 0,
                recordCounts.bqDetails?.totalRecords || summary.targetRecords || 0,
                summary.recordsReachedTarget || 0,
                summary.recordsFailedToReachTarget || 0,
                (summary.pipelineSuccessRate || '0') + '%'
            ]);
        } else {
            summaryData.push([
                idx + 1,
                r.endpoint,
                tableName,
                r.bqFilter || 'None',
                '✗ Failed',
                r.apiRecords || 0,
                'N/A',
                'N/A',
                'N/A',
                r.error || 'Unknown error'
            ]);
        }
    });
    
    const summarySheet = XLSX.utils.aoa_to_sheet(summaryData);
    summarySheet['!cols'] = [
        {wch: 5}, {wch: 50}, {wch: 30}, {wch: 40}, {wch: 12}, 
        {wch: 12}, {wch: 12}, {wch: 12}, {wch: 12}, {wch: 15}
    ];
    XLSX.utils.book_append_sheet(wb, summarySheet, 'Executive Summary');
    
    // Sheet 2: Field Analysis for each successful API
    globalMultiApiResults.forEach((r, idx) => {
        if (!r.success || !r.data) return;
        
        const tableName = r.bqTable.split('.').pop();
        const fieldWiseAnalysis = r.data.fieldWiseAnalysis || {};
        const fields = fieldWiseAnalysis.fieldResults || fieldWiseAnalysis.fieldComparison || [];
        
        if (fields.length === 0) return;
        
        const fieldData = [
            ['FIELD ANALYSIS: ' + tableName],
            ['Endpoint: ' + r.endpoint],
            ['Filter: ' + (r.bqFilter || 'None')],
            [''],
            ['Field Name', 'Match Rate', 'Matches', 'Mismatches', 'Status']
        ];
        
        fields.forEach(f => {
            const matchRate = f.matchRate || f.matchPercentage || 
                             (f.totalRecords > 0 ? ((f.perfectMatches / f.totalRecords) * 100).toFixed(1) : 100);
            const matches = f.matchCount || f.matches || f.perfectMatches || 0;
            const mismatches = f.mismatchCount || f.mismatches || f.differences || 0;
            const fieldName = f.fieldName || f.field || 'Unknown';
            const status = matchRate >= 99 ? '✓ OK' : matchRate >= 90 ? '⚠ Warning' : '✗ Issue';
            
            fieldData.push([fieldName, matchRate + '%', matches, mismatches, status]);
        });
        
        const sheetName = ('Fields_' + tableName).substring(0, 31); // Excel sheet name limit
        const fieldSheet = XLSX.utils.aoa_to_sheet(fieldData);
        fieldSheet['!cols'] = [{wch: 30}, {wch: 12}, {wch: 12}, {wch: 12}, {wch: 12}];
        XLSX.utils.book_append_sheet(wb, fieldSheet, sheetName);
    });
    
    // Sheet 3: Mismatched Records Details
    const mismatchData = [
        ['MISMATCHED RECORDS DETAILS'],
        [''],
        ['Table', 'Field', 'Primary Key', 'API Value', 'BQ Value']
    ];
    
    globalMultiApiResults.forEach((r, idx) => {
        if (!r.success || !r.data) return;
        
        const tableName = r.bqTable.split('.').pop();
        const fieldWiseAnalysis = r.data.fieldWiseAnalysis || {};
        const fields = fieldWiseAnalysis.fieldResults || fieldWiseAnalysis.fieldComparison || [];
        
        fields.forEach(f => {
            const fieldName = f.fieldName || f.field || 'Unknown';
            const sampleDifferences = f.sampleDifferences || [];
            
            sampleDifferences.forEach(d => {
                mismatchData.push([
                    tableName,
                    fieldName,
                    d.primaryKey || d.key || 'N/A',
                    String(d.apiValue || d.jsonValue || ''),
                    String(d.bqValue || d.targetValue || '')
                ]);
            });
        });
    });
    
    if (mismatchData.length > 3) {
        const mismatchSheet = XLSX.utils.aoa_to_sheet(mismatchData);
        mismatchSheet['!cols'] = [{wch: 25}, {wch: 25}, {wch: 30}, {wch: 40}, {wch: 40}];
        XLSX.utils.book_append_sheet(wb, mismatchSheet, 'Mismatched Records');
    }
    
    // Sheet 4: Missing Records
    const missingData = [
        ['MISSING RECORDS ANALYSIS'],
        [''],
        ['Table', 'Type', 'Count', 'Sample Keys']
    ];
    
    globalMultiApiResults.forEach((r, idx) => {
        if (!r.success || !r.data) return;
        
        const tableName = r.bqTable.split('.').pop();
        const summary = r.data.summary || {};
        const compResults = r.data.comparisonResults || {};
        const missingRecords = r.data.missingRecordsAnalysis || {};
        
        const apiOnly = missingRecords.apiOnlyRecords || compResults.missing?.missingFromBQ || {};
        const bqOnly = missingRecords.bqOnlyRecords || compResults.missing?.missingFromJSON || {};
        
        const apiOnlyCount = apiOnly.count || summary.recordsFailedToReachTarget || 0;
        const bqOnlyCount = bqOnly.count || summary.recordsOnlyInTarget || 0;
        
        if (apiOnlyCount > 0) {
            const sampleKeys = (apiOnly.sampleKeys || apiOnly.samples || []).slice(0, 10).join(', ');
            missingData.push([tableName, 'In API only (not in BQ)', apiOnlyCount, sampleKeys || 'N/A']);
        }
        
        if (bqOnlyCount > 0) {
            const sampleKeys = (bqOnly.sampleKeys || bqOnly.samples || []).slice(0, 10).join(', ');
            missingData.push([tableName, 'In BQ only (not in API)', bqOnlyCount, sampleKeys || 'N/A']);
        }
    });
    
    if (missingData.length > 3) {
        const missingSheet = XLSX.utils.aoa_to_sheet(missingData);
        missingSheet['!cols'] = [{wch: 25}, {wch: 25}, {wch: 12}, {wch: 60}];
        XLSX.utils.book_append_sheet(wb, missingSheet, 'Missing Records');
    }
    
    // Sheet 5: Schema Analysis
    const schemaData = [
        ['SCHEMA ANALYSIS'],
        [''],
        ['Table', 'Common Fields', 'API Only Fields', 'BQ Only Fields']
    ];
    
    globalMultiApiResults.forEach((r, idx) => {
        if (!r.success || !r.data) return;
        
        const tableName = r.bqTable.split('.').pop();
        const schemaAnalysis = r.data.schemaAnalysis || {};
        
        const commonFields = (schemaAnalysis.commonFields || []).join(', ');
        const apiOnlyFields = (schemaAnalysis.jsonOnlyFields || schemaAnalysis.tempOnlyFields || []).join(', ');
        const bqOnlyFields = (schemaAnalysis.bqOnlyFields || schemaAnalysis.sourceOnlyFields || []).join(', ');
        
        schemaData.push([tableName, commonFields || 'N/A', apiOnlyFields || 'None', bqOnlyFields || 'None']);
    });
    
    const schemaSheet = XLSX.utils.aoa_to_sheet(schemaData);
    schemaSheet['!cols'] = [{wch: 25}, {wch: 60}, {wch: 40}, {wch: 40}];
    XLSX.utils.book_append_sheet(wb, schemaSheet, 'Schema Analysis');
    
    // Generate filename and download
    const filename = `Multi_API_vs_BQ_Validation_${timestamp[0]}_${timestamp[1].split('.')[0]}.xlsx`;
    XLSX.writeFile(wb, filename);
    
    console.log(`Multi-API Excel report exported: ${filename}`);
    alert(`Report exported successfully!\n\nFilename: ${filename}\n\nYou can now attach this file to your Jira ticket.`);
}

// View detailed results for a specific API
function viewMultiApiDetailedResults(idx) {
    if (!globalMultiApiResults || !globalMultiApiResults[idx] || !globalMultiApiResults[idx].data) {
        alert('No detailed data available for this API');
        return;
    }
    
    const result = globalMultiApiResults[idx];
    const data = result.data;
    const tableName = result.bqTable.split('.').pop();
    const container = document.getElementById('multi-api-results');
    
    console.log('Detailed view data:', data); // Debug log
    
    // Build detailed view similar to single API results
    const summary = data.summary || {};
    const schemaAnalysis = data.schemaAnalysis || {};
    const recordCounts = data.recordCounts || {};
    const fieldWiseAnalysis = data.fieldWiseAnalysis || {};
    const duplicatesAnalysis = data.duplicatesAnalysis || {};
    const comparisonResults = data.comparisonResults || {};
    const missingRecordsAnalysis = data.missingRecordsAnalysis || {};
    const metadata = data.metadata || {};
    
    // Get primary key from multiple possible locations
    const primaryKey = summary.primaryKeyUsed || metadata.primaryKey || comparisonResults.primaryKeyUsed || 'N/A';
    
    // Get filter info
    const filterUsed = result.bqFilter || metadata.bqFilter || comparisonResults.filterCondition || null;
    
    // Schema info
    const commonFields = schemaAnalysis.commonFields || [];
    const jsonOnlyFields = schemaAnalysis.jsonOnlyFields || schemaAnalysis.tempOnlyFields || [];
    const bqOnlyFields = schemaAnalysis.bqOnlyFields || schemaAnalysis.sourceOnlyFields || [];
    
    // Missing records - handle both filtered and non-filtered structures
    const missingFromBQ = missingRecordsAnalysis.apiOnlyRecords || 
                          missingRecordsAnalysis.jsonOnlyRecords ||
                          comparisonResults.missing?.missingFromBQ || 
                          { count: summary.recordsFailedToReachTarget || 0, sampleKeys: [] };
    const missingFromJSON = missingRecordsAnalysis.bqOnlyRecords ||
                            comparisonResults.missing?.missingFromJSON || 
                            { count: summary.recordsOnlyInTarget || 0, sampleKeys: [] };
    
    // Record counts - handle both structures
    const apiRecordCount = recordCounts.jsonDetails?.totalRecords || 
                           recordCounts.tempDetails?.totalRecords ||
                           summary.totalRecordsInFile || 
                           result.apiRecords || 0;
    const bqRecordCount = recordCounts.bqDetails?.totalRecords || 
                          recordCounts.sourceDetails?.totalRecords ||
                          recordCounts.filteredSourceDetails?.totalRecords ||
                          summary.targetRecords || 
                          summary.filteredTargetRecords || 0;
    
    container.innerHTML = `
        <div style="padding: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h3 style="color: #495057; margin: 0;">🔍 Detailed Analysis: ${tableName}</h3>
                <button onclick="backToMultiApiSummary()" style="padding: 10px 20px; background: #6c757d; color: white; border: none; border-radius: 6px; cursor: pointer;">
                    ← Back to Summary
                </button>
            </div>
            
            <div style="background: #e7f3ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #007bff;">
                <strong>Endpoint:</strong> ${result.endpoint}<br>
                <strong>Target Table:</strong> ${result.bqTable}<br>
                <strong>Primary Key:</strong> ${primaryKey}
                ${filterUsed ? `<br><strong style="color: #e67e22;">🔍 Filter:</strong> <code style="background: #fef9e7; padding: 2px 6px; border-radius: 3px;">${filterUsed}</code>` : ''}
            </div>
            
            <!-- Summary Cards -->
            <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 25px;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 28px; font-weight: bold;">${apiRecordCount.toLocaleString()}</div>
                    <div style="font-size: 12px; opacity: 0.9;">API Records</div>
                </div>
                <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 28px; font-weight: bold;">${bqRecordCount.toLocaleString()}</div>
                    <div style="font-size: 12px; opacity: 0.9;">BQ Records${filterUsed ? ' (filtered)' : ''}</div>
                </div>
                <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 28px; font-weight: bold;">${(summary.recordsReachedTarget || summary.matchedRecords || 0).toLocaleString()}</div>
                    <div style="font-size: 12px; opacity: 0.9;">Matched</div>
                </div>
                <div style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 28px; font-weight: bold;">${(summary.recordsFailedToReachTarget || summary.unmatchedRecords || 0).toLocaleString()}</div>
                    <div style="font-size: 12px; opacity: 0.9;">API Only</div>
                </div>
                <div style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); padding: 20px; border-radius: 10px; text-align: center; color: white;">
                    <div style="font-size: 28px; font-weight: bold;">${summary.pipelineSuccessRate || summary.matchRate || '0'}%</div>
                    <div style="font-size: 12px; opacity: 0.9;">Success Rate</div>
                </div>
            </div>
            
            <!-- Tabs -->
            <div style="border-bottom: 2px solid #dee2e6; margin-bottom: 20px;">
                <div style="display: flex; gap: 5px;">
                    <button class="multi-api-detail-tab active" onclick="showMultiApiDetailTab('schema', ${idx})" data-tab="schema">📋 Schema</button>
                    <button class="multi-api-detail-tab" onclick="showMultiApiDetailTab('records', ${idx})" data-tab="records">📊 Record Counts</button>
                    <button class="multi-api-detail-tab" onclick="showMultiApiDetailTab('fields', ${idx})" data-tab="fields">🔍 Field Analysis</button>
                    <button class="multi-api-detail-tab" onclick="showMultiApiDetailTab('missing', ${idx})" data-tab="missing">⚠️ Missing Records</button>
                    <button class="multi-api-detail-tab" onclick="showMultiApiDetailTab('duplicates', ${idx})" data-tab="duplicates">🔄 Duplicates</button>
                </div>
            </div>
            
            <!-- Schema Tab -->
            <div id="multi-api-detail-schema" class="multi-api-detail-content" style="display: block;">
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px;">
                    <div style="background: #d4edda; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #155724; margin: 0 0 15px 0;">✅ Common Fields (${commonFields.length})</h4>
                        <div style="max-height: 300px; overflow-y: auto;">
                            ${commonFields.length > 0 ? commonFields.map(f => `<div style="padding: 5px 10px; background: white; margin-bottom: 5px; border-radius: 4px; font-family: monospace; font-size: 13px;">${f}</div>`).join('') : '<div style="color: #666; font-style: italic;">No common fields data</div>'}
                        </div>
                    </div>
                    <div style="background: #fff3cd; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #856404; margin: 0 0 15px 0;">📤 API Only Fields (${jsonOnlyFields.length})</h4>
                        <div style="max-height: 300px; overflow-y: auto;">
                            ${jsonOnlyFields.length > 0 ? jsonOnlyFields.map(f => `<div style="padding: 5px 10px; background: white; margin-bottom: 5px; border-radius: 4px; font-family: monospace; font-size: 13px;">${f}</div>`).join('') : '<div style="color: #666; font-style: italic;">None</div>'}
                        </div>
                    </div>
                    <div style="background: #cce5ff; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #004085; margin: 0 0 15px 0;">📥 BQ Only Fields (${bqOnlyFields.length})</h4>
                        <div style="max-height: 300px; overflow-y: auto;">
                            ${bqOnlyFields.length > 0 ? bqOnlyFields.map(f => `<div style="padding: 5px 10px; background: white; margin-bottom: 5px; border-radius: 4px; font-family: monospace; font-size: 13px;">${f}</div>`).join('') : '<div style="color: #666; font-style: italic;">None</div>'}
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Record Counts Tab -->
            <div id="multi-api-detail-records" class="multi-api-detail-content" style="display: none;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div style="background: #e7f3ff; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #004085; margin: 0 0 15px 0;">📤 API Source</h4>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 10px; border-bottom: 1px solid #dee2e6;">Total Records</td><td style="padding: 10px; border-bottom: 1px solid #dee2e6; font-weight: bold; text-align: right;">${apiRecordCount.toLocaleString()}</td></tr>
                            <tr><td style="padding: 10px; border-bottom: 1px solid #dee2e6;">Unique Keys</td><td style="padding: 10px; border-bottom: 1px solid #dee2e6; font-weight: bold; text-align: right;">${(recordCounts.jsonDetails?.uniquePrimaryKeys || recordCounts.tempDetails?.uniqueKeys || apiRecordCount).toLocaleString()}</td></tr>
                            <tr><td style="padding: 10px;">Duplicate Records</td><td style="padding: 10px; font-weight: bold; text-align: right;">${(recordCounts.jsonDetails?.duplicateRecords || recordCounts.tempDetails?.duplicates || 0).toLocaleString()}</td></tr>
                        </table>
                    </div>
                    <div style="background: #e8f5e9; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #2e7d32; margin: 0 0 15px 0;">📥 BigQuery Target${filterUsed ? ' (Filtered)' : ''}</h4>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr><td style="padding: 10px; border-bottom: 1px solid #dee2e6;">Total Records</td><td style="padding: 10px; border-bottom: 1px solid #dee2e6; font-weight: bold; text-align: right;">${bqRecordCount.toLocaleString()}</td></tr>
                            <tr><td style="padding: 10px; border-bottom: 1px solid #dee2e6;">Unique Keys</td><td style="padding: 10px; border-bottom: 1px solid #dee2e6; font-weight: bold; text-align: right;">${(recordCounts.bqDetails?.uniquePrimaryKeys || recordCounts.sourceDetails?.uniqueKeys || recordCounts.filteredSourceDetails?.uniqueKeys || bqRecordCount).toLocaleString()}</td></tr>
                            <tr><td style="padding: 10px;">Duplicate Records</td><td style="padding: 10px; font-weight: bold; text-align: right;">${(recordCounts.bqDetails?.duplicateRecords || recordCounts.sourceDetails?.duplicates || recordCounts.filteredSourceDetails?.duplicates || 0).toLocaleString()}</td></tr>
                        </table>
                    </div>
                </div>
            </div>
            
            <!-- Field Analysis Tab -->
            <div id="multi-api-detail-fields" class="multi-api-detail-content" style="display: none;">
                ${buildFieldAnalysisHtml(fieldWiseAnalysis)}
            </div>
            
            <!-- Missing Records Tab -->
            <div id="multi-api-detail-missing" class="multi-api-detail-content" style="display: none;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div style="background: #f8d7da; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #721c24; margin: 0 0 15px 0;">🔴 In API but NOT in BQ (${missingFromBQ.count || summary.recordsFailedToReachTarget || 0})</h4>
                        <div style="max-height: 400px; overflow-y: auto;">
                            ${(missingFromBQ.sampleKeys || missingFromBQ.samples || []).length > 0 ? 
                                `<table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                                    <thead><tr style="background: #f5c6cb;"><th style="padding: 8px; text-align: left;">#</th><th style="padding: 8px; text-align: left;">Key Value</th></tr></thead>
                                    <tbody>${(missingFromBQ.sampleKeys || missingFromBQ.samples || []).slice(0, 100).map((k, i) => `<tr style="border-bottom: 1px solid #eee;"><td style="padding: 6px;">${i+1}</td><td style="padding: 6px; font-family: monospace;">${k}</td></tr>`).join('')}</tbody>
                                </table>` : 
                                '<div style="color: #155724; padding: 20px; text-align: center;">✅ No missing records</div>'}
                        </div>
                    </div>
                    <div style="background: #cce5ff; padding: 20px; border-radius: 8px;">
                        <h4 style="color: #004085; margin: 0 0 15px 0;">🔵 In BQ but NOT in API (${missingFromJSON.count || summary.recordsOnlyInTarget || 0})</h4>
                        <div style="max-height: 400px; overflow-y: auto;">
                            ${(missingFromJSON.sampleKeys || missingFromJSON.samples || []).length > 0 ? 
                                `<table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                                    <thead><tr style="background: #b8daff;"><th style="padding: 8px; text-align: left;">#</th><th style="padding: 8px; text-align: left;">Key Value</th></tr></thead>
                                    <tbody>${(missingFromJSON.sampleKeys || missingFromJSON.samples || []).slice(0, 100).map((k, i) => `<tr style="border-bottom: 1px solid #eee;"><td style="padding: 6px;">${i+1}</td><td style="padding: 6px; font-family: monospace;">${k}</td></tr>`).join('')}</tbody>
                                </table>` : 
                                '<div style="color: #155724; padding: 20px; text-align: center;">✅ No extra records in BQ</div>'}
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Duplicates Tab -->
            <div id="multi-api-detail-duplicates" class="multi-api-detail-content" style="display: none;">
                ${buildDuplicatesAnalysisHtml(duplicatesAnalysis)}
            </div>
        </div>
    `;
    
    window.scrollTo({top: 0, behavior: 'smooth'});
}

// Helper function to build field analysis HTML
function buildFieldAnalysisHtml(fieldWiseAnalysis) {
    // Handle both filtered and non-filtered structures
    const fields = fieldWiseAnalysis?.fieldResults || fieldWiseAnalysis?.fieldComparison || [];
    
    if (!fieldWiseAnalysis || fields.length === 0) {
        return '<div style="padding: 40px; text-align: center; color: #666;">No field-wise analysis data available</div>';
    }
    
    let html = '<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse; font-size: 14px;">';
    html += '<thead><tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">';
    html += '<th style="padding: 12px; text-align: left;">Field Name</th>';
    html += '<th style="padding: 12px; text-align: center;">Match Rate</th>';
    html += '<th style="padding: 12px; text-align: center;">Matches</th>';
    html += '<th style="padding: 12px; text-align: center;">Mismatches</th>';
    html += '<th style="padding: 12px; text-align: center;">Status</th>';
    html += '<th style="padding: 12px; text-align: center;">Details</th>';
    html += '</tr></thead><tbody>';
    
    fields.forEach((f, i) => {
        const matchRate = f.matchRate || f.matchPercentage || 
                         (f.totalRecords > 0 ? ((f.perfectMatches / f.totalRecords) * 100).toFixed(1) : 100);
        const matches = f.matchCount || f.matches || f.perfectMatches || 0;
        const mismatches = f.mismatchCount || f.mismatches || f.differences || 0;
        const fieldName = f.fieldName || f.field || 'Unknown';
        
        const sampleMatches = f.sampleMatches || f.matchedSamples || [];
        const sampleDifferences = f.sampleDifferences || f.differenceSamples || f.mismatchedRecords || [];
        
        const statusColor = matchRate >= 99 ? '#28a745' : matchRate >= 90 ? '#ffc107' : '#dc3545';
        const statusIcon = matchRate >= 99 ? '✅' : matchRate >= 90 ? '⚠️' : '❌';
        const rowId = 'field-detail-' + i;
        const hasMismatches = mismatches > 0;
        const bgColor = i % 2 === 0 ? '#f8f9fa' : 'white';
        
        html += '<tr style="background: ' + bgColor + '; border-bottom: 1px solid #dee2e6;">';
        html += '<td style="padding: 10px; font-family: monospace;">' + fieldName + '</td>';
        html += '<td style="padding: 10px; text-align: center;"><span style="background: ' + statusColor + '; color: white; padding: 3px 10px; border-radius: 12px; font-weight: bold;">' + matchRate + '%</span></td>';
        html += '<td style="padding: 10px; text-align: center; color: #28a745; font-weight: bold;">' + matches.toLocaleString() + '</td>';
        html += '<td style="padding: 10px; text-align: center; color: #dc3545; font-weight: bold;">' + mismatches.toLocaleString() + '</td>';
        html += '<td style="padding: 10px; text-align: center;">' + statusIcon + '</td>';
        html += '<td style="padding: 10px; text-align: center;">';
        html += '<button onclick="toggleFieldDetail(\'' + rowId + '\')" style="padding: 5px 10px; background: ' + (hasMismatches ? '#dc3545' : '#28a745') + '; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">';
        html += hasMismatches ? '🔍 View Diff' : '✅ Samples';
        html += '</button></td></tr>';
        
        // Detail row
        html += '<tr id="' + rowId + '" style="display: none;">';
        html += '<td colspan="6" style="padding: 0; background: #f8f9fa;">';
        html += '<div style="padding: 15px; border: 2px solid ' + (hasMismatches ? '#dc3545' : '#28a745') + '; border-radius: 0 0 8px 8px; margin: 0 10px 10px 10px;">';
        
        if (hasMismatches) {
            html += '<h5 style="color: #dc3545; margin: 0 0 10px 0;">❌ Mismatched Records for "' + fieldName + '" (' + mismatches + ' differences)</h5>';
            if (sampleDifferences.length > 0) {
                html += '<div style="max-height: 300px; overflow-y: auto;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #f5c6cb;">';
                html += '<th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Primary Key</th>';
                html += '<th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">API Value</th>';
                html += '<th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">BQ Value</th>';
                html += '</tr></thead><tbody>';
                sampleDifferences.slice(0, 20).forEach(d => {
                    html += '<tr>';
                    html += '<td style="padding: 6px; border: 1px solid #dee2e6; font-family: monospace;">' + (d.primaryKey || d.key || d.id || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #dee2e6; background: #ffe6e6; font-family: monospace; word-break: break-all;">' + formatFieldValue(d.apiValue || d.jsonValue || d.sourceValue) + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #dee2e6; background: #e6f3ff; font-family: monospace; word-break: break-all;">' + formatFieldValue(d.bqValue || d.targetValue) + '</td>';
                    html += '</tr>';
                });
                html += '</tbody></table>';
                if (sampleDifferences.length > 20) {
                    html += '<div style="color: #666; font-style: italic; padding: 10px; text-align: center;">Showing first 20 of ' + sampleDifferences.length + ' differences</div>';
                }
                html += '</div>';
            } else {
                html += '<div style="color: #666; padding: 10px;">' + mismatches + ' records have different values. Sample data not available.</div>';
            }
        } else {
            html += '<h5 style="color: #28a745; margin: 0 0 10px 0;">✅ All ' + matches + ' Records Match for "' + fieldName + '"</h5>';
            if (sampleMatches.length > 0) {
                html += '<div style="max-height: 300px; overflow-y: auto;">';
                html += '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">';
                html += '<thead><tr style="background: #d4edda;">';
                html += '<th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Primary Key</th>';
                html += '<th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Value (Same in API & BQ)</th>';
                html += '</tr></thead><tbody>';
                sampleMatches.slice(0, 10).forEach(m => {
                    html += '<tr>';
                    html += '<td style="padding: 6px; border: 1px solid #dee2e6; font-family: monospace;">' + (m.primaryKey || m.key || m.id || 'N/A') + '</td>';
                    html += '<td style="padding: 6px; border: 1px solid #dee2e6; font-family: monospace; word-break: break-all;">' + formatFieldValue(m.value || m.apiValue || m.matchedValue) + '</td>';
                    html += '</tr>';
                });
                html += '</tbody></table>';
                if (sampleMatches.length > 10) {
                    html += '<div style="color: #666; font-style: italic; padding: 10px; text-align: center;">Showing first 10 of ' + matches + ' matched records</div>';
                }
                html += '</div>';
            } else {
                html += '<div style="color: #28a745; padding: 10px;">✅ All ' + matches + ' records have identical values in both API and BigQuery.</div>';
            }
        }
        
        html += '</div></td></tr>';
    });
    
    html += '</tbody></table></div>';
    return html;
}

// Toggle field detail row visibility
function toggleFieldDetail(rowId) {
    const row = document.getElementById(rowId);
    if (row) {
        row.style.display = row.style.display === 'none' ? 'table-row' : 'none';
    }
}

// Format field value for display
function formatFieldValue(value) {
    if (value === null || value === undefined) return '<span style="color: #999; font-style: italic;">NULL</span>';
    if (value === '') return '<span style="color: #999; font-style: italic;">(empty)</span>';
    if (typeof value === 'object') return JSON.stringify(value);
    const strValue = String(value);
    if (strValue.length > 100) return strValue.substring(0, 100) + '...';
    return strValue;
}

// Helper function to build duplicates analysis HTML
function buildDuplicatesAnalysisHtml(duplicatesAnalysis) {
    if (!duplicatesAnalysis) {
        return '<div style="padding: 40px; text-align: center; color: #666;">No duplicates analysis data available</div>';
    }
    
    const jsonDuplicates = duplicatesAnalysis.jsonDuplicates || duplicatesAnalysis.sourceDuplicates || {};
    const bqDuplicates = duplicatesAnalysis.bqDuplicates || duplicatesAnalysis.targetDuplicates || {};
    
    return `
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
            <div style="background: #fff3cd; padding: 20px; border-radius: 8px;">
                <h4 style="color: #856404; margin: 0 0 15px 0;">📤 API Duplicates</h4>
                <div style="font-size: 32px; font-weight: bold; color: #856404; margin-bottom: 10px;">${(jsonDuplicates.count || jsonDuplicates.duplicateCount || 0).toLocaleString()}</div>
                <div style="color: #666;">duplicate records found</div>
                ${(jsonDuplicates.sampleKeys || jsonDuplicates.samples || []).length > 0 ? `
                    <div style="margin-top: 15px; max-height: 200px; overflow-y: auto;">
                        <strong>Sample duplicate keys:</strong>
                        ${(jsonDuplicates.sampleKeys || jsonDuplicates.samples || []).slice(0, 10).map(k => `<div style="padding: 5px; background: white; margin-top: 5px; border-radius: 4px; font-family: monospace; font-size: 12px;">${k}</div>`).join('')}
                    </div>
                ` : ''}
            </div>
            <div style="background: #cce5ff; padding: 20px; border-radius: 8px;">
                <h4 style="color: #004085; margin: 0 0 15px 0;">📥 BigQuery Duplicates</h4>
                <div style="font-size: 32px; font-weight: bold; color: #004085; margin-bottom: 10px;">${(bqDuplicates.count || bqDuplicates.duplicateCount || 0).toLocaleString()}</div>
                <div style="color: #666;">duplicate records found</div>
                ${(bqDuplicates.sampleKeys || bqDuplicates.samples || []).length > 0 ? `
                    <div style="margin-top: 15px; max-height: 200px; overflow-y: auto;">
                        <strong>Sample duplicate keys:</strong>
                        ${(bqDuplicates.sampleKeys || bqDuplicates.samples || []).slice(0, 10).map(k => `<div style="padding: 5px; background: white; margin-top: 5px; border-radius: 4px; font-family: monospace; font-size: 12px;">${k}</div>`).join('')}
                    </div>
                ` : ''}
            </div>
        </div>
    `;
}

// Show multi-API detail tab
function showMultiApiDetailTab(tabName, idx) {
    document.querySelectorAll('.multi-api-detail-content').forEach(c => c.style.display = 'none');
    document.querySelectorAll('.multi-api-detail-tab').forEach(t => t.classList.remove('active'));
    
    const content = document.getElementById(`multi-api-detail-${tabName}`);
    if (content) content.style.display = 'block';
    
    const tab = document.querySelector(`.multi-api-detail-tab[data-tab="${tabName}"]`);
    if (tab) tab.classList.add('active');
}

// Back to multi-API summary
function backToMultiApiSummary() {
    if (globalMultiApiResults) {
        const container = document.getElementById('multi-api-results');
        displayMultiApiResults(globalMultiApiResults, container);
    }
    window.scrollTo({top: 0, behavior: 'smooth'});
}

        // API Results Tab Navigation

        function showApiResultsTab(tabId) {
            const resultsTabs = document.querySelectorAll('.results-tab-content');
            resultsTabs.forEach(tab => tab.classList.remove('active'));

            const resultsTabButtons = document.querySelectorAll('.results-tab-button');
            resultsTabButtons.forEach(button => button.classList.remove('active'));

            const selectedTab = document.getElementById(tabId + '-tab');
            if (selectedTab) selectedTab.classList.add('active');

            const activeTabButton = event.target.closest('.results-tab-button');
            if (activeTabButton) activeTabButton.classList.add('active');

            console.log(`Switched to API results tab: ${tabId}`);
        }

        // Enhanced API Results Display
        function displayApiComparisonResults(results) {
            console.log('Displaying enhanced API comparison results:', results);

            globalApiResults = results;

            const exportButton = document.getElementById('exportApiToExcel');
            if (exportButton) {
                exportButton.disabled = false;
            }

            // Populate all tabs with comprehensive data
            populateApiRecordCountTab(results.recordCounts, results.summary);
            populateApiColumnNamesTab(results.schemaAnalysis);
            populateApiComparisonTab(results.comparisonResults, results.summary, results.metadata);
            populateApiFieldWiseTab(results.fieldWiseAnalysis, results.summary);
            populateApiDuplicatesTab(results.duplicatesAnalysis, results.summary);
            
            // NEW: Populate missing records analysis
            populateApiMissingRecordsTab(results.missingRecordsAnalysis, results.summary);
        }
        
        // NEW: Display missing records analysis
        function populateApiMissingRecordsTab(missingRecordsAnalysis, summary) {
            const container = document.getElementById('api-duplicates-summary');
            if (!container || !missingRecordsAnalysis) return;
            
            const apiOnly = missingRecordsAnalysis.apiOnlyRecords || { count: 0, sampleKeys: [] };
            const bqOnly = missingRecordsAnalysis.bqOnlyRecords || { count: 0, sampleKeys: [] };
            const missingSummary = missingRecordsAnalysis.summary || {};
            
            // Only add if there are missing records
            if (apiOnly.count === 0 && bqOnly.count === 0) {
                // Add a success message
                const successDiv = document.createElement('div');
                successDiv.style.cssText = 'margin-top: 20px; width: 100%;';
                successDiv.innerHTML = `
                    <div style="background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); padding: 20px; border-radius: 12px; border-left: 5px solid #28a745;">
                        <h4 style="color: #155724; margin: 0 0 10px 0;">✅ Perfect Record Match</h4>
                        <p style="color: #155724; margin: 0;">All records in API exist in BigQuery and vice versa. No missing records detected.</p>
                    </div>
                `;
                container.appendChild(successDiv);
                return;
            }
            
            // Create a full-width container for missing records
            const missingDiv = document.createElement('div');
            missingDiv.style.cssText = 'margin-top: 20px; width: 100%; grid-column: 1 / -1;';
            
            let missingHtml = `
                <div style="background: linear-gradient(135deg, #fff3cd 0%, #ffeeba 100%); padding: 20px; border-radius: 12px; border-left: 5px solid #ffc107; width: 100%;">
                    <h4 style="color: #856404; margin: 0 0 15px 0;">⚠️ Missing Records Analysis</h4>
                    <p style="color: #856404; margin-bottom: 15px;">Found <strong>${missingSummary.totalMismatches || 0}</strong> record(s) that don't match between API and BigQuery.</p>
            `;
            
            // API Only Records
            if (apiOnly.count > 0) {
                missingHtml += `
                    <div style="background: white; padding: 15px; border-radius: 8px; border: 2px solid #e74c3c; margin-bottom: 15px;">
                        <h5 style="color: #e74c3c; margin: 0 0 10px 0;">🔴 In API but NOT in BigQuery: ${apiOnly.count} record(s)</h5>
                        <p style="font-size: 0.85rem; color: #666; margin-bottom: 10px;">${apiOnly.description}</p>
                        <div style="max-height: 300px; overflow-y: auto; background: #f8f9fa; padding: 10px; border-radius: 4px;">
                            <table style="width: 100%; border-collapse: collapse; font-family: monospace; font-size: 0.85rem;">
                                <thead>
                                    <tr style="background: #e9ecef;">
                                        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">#</th>
                                        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">metric_id (Missing from BigQuery)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${apiOnly.sampleKeys.map((key, idx) => `
                                        <tr style="border-bottom: 1px solid #eee;">
                                            <td style="padding: 6px 8px; color: #666;">${idx + 1}</td>
                                            <td style="padding: 6px 8px; word-break: break-all;">${key}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                            ${apiOnly.truncated ? '<div style="color: #999; font-style: italic; padding: 10px; text-align: center;">... showing first 100 of ' + apiOnly.count + ' records</div>' : ''}
                        </div>
                    </div>
                `;
            }
            
            // BQ Only Records
            if (bqOnly.count > 0) {
                missingHtml += `
                    <div style="background: white; padding: 15px; border-radius: 8px; border: 2px solid #3498db;">
                        <h5 style="color: #3498db; margin: 0 0 10px 0;">🔵 In BigQuery but NOT in API: ${bqOnly.count} record(s)</h5>
                        <p style="font-size: 0.85rem; color: #666; margin-bottom: 10px;">${bqOnly.description}</p>
                        <div style="max-height: 300px; overflow-y: auto; background: #f8f9fa; padding: 10px; border-radius: 4px;">
                            <table style="width: 100%; border-collapse: collapse; font-family: monospace; font-size: 0.85rem;">
                                <thead>
                                    <tr style="background: #e9ecef;">
                                        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">#</th>
                                        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">metric_id (Missing from API)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${bqOnly.sampleKeys.map((key, idx) => `
                                        <tr style="border-bottom: 1px solid #eee;">
                                            <td style="padding: 6px 8px; color: #666;">${idx + 1}</td>
                                            <td style="padding: 6px 8px; word-break: break-all;">${key}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                            ${bqOnly.truncated ? '<div style="color: #999; font-style: italic; padding: 10px; text-align: center;">... showing first 100 of ' + bqOnly.count + ' records</div>' : ''}
                        </div>
                    </div>
                `;
            }
            
            missingHtml += `</div>`;
            
            missingDiv.innerHTML = missingHtml;
            container.appendChild(missingDiv);
        }
        
// Replace the populateApiRecordCountTab function in index.html with this enhanced version
// that displays BigQuery filter information

function populateApiRecordCountTab(recordCounts, summary) {
    // Get filter information
    const filterApplied = summary?.bigQueryFilterApplied || false;
    const filterCondition = summary?.bigQueryFilterCondition || null;
    const originalBqRecords = summary?.originalBigQueryRecords || null;
    const filterReduction = summary?.filterReduction || null;

    // FIXED: Use totalRecordsInAPI (total available) instead of comparison records
    const apiTotal = recordCounts?.apiDetails?.totalRecordsInAPI ||
                     summary?.totalRecordsInAPI ||
                     summary?.totalRecordsInFile || 0;

    const bqTotal = recordCounts?.bqDetails?.totalRecords || summary?.targetRecords || 0;
    const comparisonRecords = recordCounts?.apiDetails?.recordsForComparison ||
                             summary?.recordsForComparison || 20;

    const apiUnique = recordCounts?.apiDetails?.uniquePrimaryKeys || summary?.uniqueSourceRecords || 0;
    const bqUnique = recordCounts?.bqDetails?.uniquePrimaryKeys || 0;
    const apiNulls = recordCounts?.apiDetails?.nullPrimaryKeys || 0;
    const bqNulls = recordCounts?.bqDetails?.nullPrimaryKeys || 0;
    const apiDuplicates = recordCounts?.apiDetails?.duplicateRecords || summary?.duplicateRecordsInFile || 0;

    const recordCountSummary = document.getElementById('api-record-count-summary');
    if (recordCountSummary) {
        let summaryHTML = `
            <div class="summary-card">
                <div class="summary-number">${apiTotal.toLocaleString()}</div>
                <div class="summary-label">API Total Records</div>
            </div>
            <div class="summary-card ${filterApplied ? 'warning' : ''}">
                <div class="summary-number">${bqTotal.toLocaleString()}</div>
                <div class="summary-label">${filterApplied ? 'BigQuery Filtered' : 'BigQuery Total Records'}</div>
            </div>
            <div class="summary-card matches">
                <div class="summary-number">${apiUnique.toLocaleString()}</div>
                <div class="summary-label">API Unique Keys</div>
            </div>
            <div class="summary-card matches">
                <div class="summary-number">${bqUnique.toLocaleString()}</div>
                <div class="summary-label">BigQuery Unique Keys</div>
            </div>
            <div class="summary-card ${apiDuplicates > 0 ? 'warning' : 'pass'}">
                <div class="summary-number">${apiDuplicates.toLocaleString()}</div>
                <div class="summary-label">API Duplicates</div>
            </div>
        `;

        // Add filter reduction card if filter was applied
        if (filterApplied && filterReduction) {
            summaryHTML += `
                <div class="summary-card differences">
                    <div class="summary-number">${filterReduction.percentageReduced}%</div>
                    <div class="summary-label">Filter Reduction</div>
                </div>
            `;
        } else {
            summaryHTML += `
                <div class="summary-card warning">
                    <div class="summary-number">${apiNulls + bqNulls}</div>
                    <div class="summary-label">Total Null Keys</div>
                </div>
            `;
        }

        recordCountSummary.innerHTML = summaryHTML;
    }

    const recordCountDetails = document.getElementById('api-record-count-details');
    if (recordCountDetails) {
        const primaryKeyField = recordCounts?.apiDetails?.primaryKeyField || summary?.primaryKeyUsed || 'custom field';

        let detailsHTML = `
            <div style="background: #e8f5e8; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #27ae60;">
                <h4 style="color: #2c3e50; margin-bottom: 10px;">✅ Primary Key Analysis</h4>
                <p><strong>Primary Key Used:</strong> <code>${primaryKeyField}</code> (API field matching BigQuery)</p>
                <p><strong>Comparison Strategy:</strong> Using ${comparisonRecords} sample records from ${apiTotal} total API records for efficient analysis</p>
            </div>
        `;

        // Add BigQuery filter information if applied
        if (filterApplied) {
            detailsHTML += `
                <div style="background: #e8f4fd; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                    <h4 style="color: #2c3e50; margin-bottom: 10px;">🔍 BigQuery Filter Applied</h4>
                    <p><strong>Filter Condition:</strong></p>
                    <div style="background: #f8f9fa; padding: 10px; border-radius: 4px; font-family: monospace; font-size: 0.9rem; margin: 8px 0;">
                        ${filterCondition}
                    </div>
                    ${originalBqRecords ? `
                        <p><strong>Filter Impact:</strong></p>
                        <ul style="margin: 8px 0; padding-left: 20px;">
                            <li>Original BigQuery records: <strong>${originalBqRecords.toLocaleString()}</strong></li>
                            <li>After filtering: <strong>${bqTotal.toLocaleString()}</strong></li>
                            <li>Records removed: <strong>${filterReduction?.recordsRemoved?.toLocaleString() || 0}</strong> (${filterReduction?.percentageReduced || 0}%)</li>
                        </ul>
                    ` : ''}
                    <p style="font-style: italic; color: #666; margin-top: 10px;">
                        Comparison performed against filtered BigQuery data only
                    </p>
                </div>
            `;
        }

        detailsHTML += `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px;">
                <div>
                    <h4 style="color: #3498db; margin-bottom: 15px;">🔗 API Source Analysis</h4>
                    <table style="width: 100%; font-size: 0.9rem;">
                        <tbody>
                            <tr><td><strong>Total Records:</strong></td><td>${apiTotal.toLocaleString()}</td></tr>
                            <tr><td><strong>Unique Primary Keys:</strong></td><td>${apiUnique.toLocaleString()}</td></tr>
                            <tr><td><strong>Duplicate Records:</strong></td><td style="color: ${apiDuplicates > 0 ? '#f39c12' : '#27ae60'}; font-weight: 600;">${apiDuplicates.toLocaleString()} ${apiDuplicates > 0 ? '⚠️' : '✅'}</td></tr>
                            <tr><td><strong>Primary Key Field:</strong></td><td><code>${primaryKeyField}</code></td></tr>
                            <tr><td><strong>Sample Size for Analysis:</strong></td><td style="color: #666; font-style: italic;">${comparisonRecords.toLocaleString()} records</td></tr>
                        </tbody>
                    </table>
                </div>
                <div>
                    <h4 style="color: #2980b9; margin-bottom: 15px;">🗄️ BigQuery Target Analysis</h4>
                    <table style="width: 100%; font-size: 0.9rem;">
                        <tbody>
                            <tr><td><strong>${filterApplied ? 'Filtered' : 'Total'} Records:</strong></td><td>${bqTotal.toLocaleString()}</td></tr>
                            <tr><td><strong>Unique Primary Keys:</strong></td><td>${bqUnique.toLocaleString()}</td></tr>
                            <tr><td><strong>Primary Key Field:</strong></td><td><code>${primaryKeyField}</code></td></tr>
                            <tr><td><strong>Filter Applied:</strong></td><td style="color: ${filterApplied ? '#3498db' : '#666'}; font-weight: 600;">${filterApplied ? '🔍 Yes' : '➖ None'}</td></tr>
                            ${originalBqRecords && filterApplied ? `
                                <tr><td><strong>Original Records:</strong></td><td style="color: #666; font-style: italic;">${originalBqRecords.toLocaleString()}</td></tr>
                            ` : ''}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        // Add comparison result with filter context
        if (filterApplied) {
            if (apiTotal === bqTotal) {
                detailsHTML += `
                    <div style="background: #d4edda; padding: 15px; border-radius: 8px; margin-top: 20px; border-left: 4px solid #28a745;">
                        <h4 style="color: #155724; margin-bottom: 10px;">🎉 Perfect Match with Filtered Data!</h4>
                        <p>API records (${apiTotal.toLocaleString()}) exactly match the filtered BigQuery results (${bqTotal.toLocaleString()}).</p>
                        <p style="font-style: italic;">Filter condition: <code>${filterCondition}</code></p>
                        <p style="font-style: italic;">This indicates excellent data synchronization for the specific filter criteria.</p>
                    </div>
                `;
            } else {
                detailsHTML += `
                    <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin-top: 20px; border-left: 4px solid #ffc107;">
                        <h4 style="color: #856404; margin-bottom: 10px;">⚠️ Count Mismatch with Filtered BigQuery Data</h4>
                        <p><strong>API Records:</strong> ${apiTotal.toLocaleString()}</p>
                        <p><strong>Filtered BigQuery Records:</strong> ${bqTotal.toLocaleString()}</p>
                        <p><strong>Difference:</strong> ${Math.abs(apiTotal - bqTotal).toLocaleString()} records</p>
                        <p><strong>Filter Applied:</strong> <code>${filterCondition}</code></p>
                        <p style="margin-top: 10px; font-style: italic;">This suggests the API data may not fully match the filtered BigQuery criteria, or there may be data synchronization gaps.</p>
                    </div>
                `;
            }
        } else {
            // No filter applied - show standard comparison
            if (apiTotal === bqTotal) {
                detailsHTML += `
                    <div style="background: #d4edda; padding: 15px; border-radius: 8px; margin-top: 20px; border-left: 4px solid #28a745;">
                        <h4 style="color: #155724; margin-bottom: 10px;">🎉 Perfect Record Count Match!</h4>
                        <p>API and BigQuery both contain exactly <strong>${apiTotal.toLocaleString()}</strong> records.</p>
                        <p style="font-style: italic;">This indicates excellent data synchronization between systems.</p>
                    </div>
                `;
            } else {
                detailsHTML += `
                    <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin-top: 20px; border-left: 4px solid #ffc107;">
                        <h4 style="color: #856404; margin-bottom: 10px;">⚠️ Record Count Mismatch Detected</h4>
                        <p><strong>API Records:</strong> ${apiTotal.toLocaleString()}</p>
                        <p><strong>BigQuery Records:</strong> ${bqTotal.toLocaleString()}</p>
                        <p><strong>Difference:</strong> ${Math.abs(apiTotal - bqTotal).toLocaleString()} records</p>
                        <p style="margin-top: 10px; font-style: italic;">This suggests data synchronization issues between the API source and BigQuery target.</p>
                    </div>
                `;
            }
        }

        recordCountDetails.innerHTML = detailsHTML;
    }
}

        function populateApiColumnNamesTab(schemaAnalysis) {
            if (!schemaAnalysis) {
                const columnNamesSummary = document.getElementById('api-column-names-summary');
                if (columnNamesSummary) {
                    columnNamesSummary.innerHTML = '<div class="summary-card warning"><div class="summary-number">⚠</div><div class="summary-label">Schema Analysis Failed</div></div>';
                }
                return;
            }

            const commonFields = schemaAnalysis.commonFields || [];
            const apiOnlyFields = schemaAnalysis.apiOnlyFields || [];
            const bqOnlyFields = schemaAnalysis.bqOnlyFields || [];
            const primaryKeyCandidates = schemaAnalysis.primaryKeyCandidates || [];

            const columnNamesSummary = document.getElementById('api-column-names-summary');
            if (columnNamesSummary) {
                columnNamesSummary.innerHTML = `
                    <div class="summary-card">
                        <div class="summary-number">${schemaAnalysis.totalApiFields || 0}</div>
                        <div class="summary-label">API Fields</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-number">${schemaAnalysis.totalBqFields || 0}</div>
                        <div class="summary-label">BigQuery Fields</div>
                    </div>
                    <div class="summary-card matches">
                        <div class="summary-number">${commonFields.length}</div>
                        <div class="summary-label">Common Fields</div>
                    </div>
                    <div class="summary-card missing">
                        <div class="summary-number">${apiOnlyFields.length}</div>
                        <div class="summary-label">API Only</div>
                    </div>
                    <div class="summary-card missing">
                        <div class="summary-number">${bqOnlyFields.length}</div>
                        <div class="summary-label">BigQuery Only</div>
                    </div>
                `;
            }

            const columnComparisonGrid = document.getElementById('api-column-comparison-grid');
            if (columnComparisonGrid) {
                let commonFieldsHTML = '<div class="column-list"><h4 style="color: #27ae60;">🤝 Common Fields</h4>';
                if (commonFields.length > 0) {
                    commonFields.forEach(field => {
                        const isPrimaryKeyCandidate = primaryKeyCandidates.includes(field);
                        const icon = isPrimaryKeyCandidate ? '🔑' : '📋';
                        const cssClass = isPrimaryKeyCandidate ? 'primary-key' : 'common';
                        commonFieldsHTML += `<div class="column-item ${cssClass}">${icon} ${field}</div>`;
                    });
                } else {
                    commonFieldsHTML += '<div class="column-item">No common fields found</div>';
                }
                commonFieldsHTML += '</div>';

                let apiOnlyHTML = '<div class="column-list"><h4 style="color: #e74c3c;">🔗 API-Only Fields</h4>';
                if (apiOnlyFields.length > 0) {
                    apiOnlyFields.forEach(field => {
                        apiOnlyHTML += `<div class="column-item json-only">🔗 ${field}</div>`;
                    });
                } else {
                    apiOnlyHTML += '<div class="column-item">No API-only fields</div>';
                }
                apiOnlyHTML += '</div>';

                let bqOnlyHTML = '<div class="column-list"><h4 style="color: #f39c12;">🗄️ BigQuery-Only Fields</h4>';
                if (bqOnlyFields.length > 0) {
                    bqOnlyFields.forEach(field => {
                        bqOnlyHTML += `<div class="column-item bq-only">🗄️ ${field}</div>`;
                    });
                } else {
                    bqOnlyHTML += '<div class="column-item">No BigQuery-only fields</div>';
                }
                bqOnlyHTML += '</div>';

                columnComparisonGrid.innerHTML = commonFieldsHTML + apiOnlyHTML + bqOnlyHTML;
            }
        }

        // Replace the populateApiComparisonTab function in index.html with this enhanced version
// that shows BigQuery filter information in the strategy section

function populateApiComparisonTab(comparisonResults, summary, metadata) {
    const primaryKeyUsed = summary?.primaryKeyUsed || metadata?.primaryKey || 'unknown';
    const sourceCount = summary.totalRecordsInFile || 0;
    const uniqueCount = summary.uniqueSourceRecords || 0;
    const targetCount = summary.targetRecords || 0;
    const matchCount = summary.recordsReachedTarget || 0;
    const failedCount = summary.recordsFailedToReachTarget || 0;
    const successRate = summary.pipelineSuccessRate || '0.0';

    // Get filter information
    const filterApplied = summary?.bigQueryFilterApplied || false;
    const filterCondition = summary?.bigQueryFilterCondition || null;
    const originalTargetCount = summary?.originalBigQueryRecords || null;
    const filterReduction = summary?.filterReduction || null;

    const strategyInfo = document.getElementById('api-strategy-info');
    if (strategyInfo) {
        let strategyDescription = '';
        let insightsList = '';

        if (filterApplied) {
            // Enhanced strategy description with filter information
            strategyDescription = `Analyzing data transfer from API Source (${sourceCount} records) → Filtered BigQuery Target (${targetCount} records) using primary key: <strong>${primaryKeyUsed}</strong>`;

            insightsList = `
                <li>Using primary key: <strong>${primaryKeyUsed}</strong> (validated to exist in both systems)</li>
                <li>BigQuery Filter Applied: <code>${filterCondition}</code></li>
                ${originalTargetCount ? `
                    <li>Filter Impact: Reduced BigQuery records from ${originalTargetCount.toLocaleString()} to ${targetCount.toLocaleString()} (${filterReduction?.percentageReduced || 0}% reduction)</li>
                ` : ''}
                <li>Schema compatibility: ${summary.schemaCompatibility || 'Unknown'}% (${summary.commonFieldsCount || 0} common fields)</li>
                <li>Pipeline success rate: ${successRate}% (${matchCount}/${uniqueCount} unique records found in filtered target)</li>
                <li>Field analysis: ${summary.fieldsAnalyzed || 0} common fields analyzed across ${matchCount} matched records with filtered BigQuery data</li>
            `;
        } else {
            // Standard strategy description (no filter)
            strategyDescription = `Analyzing data transfer from API Source (${sourceCount} records) → BigQuery Target (${targetCount} records) using primary key: <strong>${primaryKeyUsed}</strong>`;

            insightsList = `
                <li>Using primary key: <strong>${primaryKeyUsed}</strong> (validated to exist in both systems)</li>
                <li>Schema compatibility: ${summary.schemaCompatibility || 'Unknown'}% (${summary.commonFieldsCount || 0} common fields)</li>
                <li>Pipeline success rate: ${successRate}% (${matchCount}/${uniqueCount} unique records found in target)</li>
                <li>Field analysis: ${summary.fieldsAnalyzed || 0} common fields analyzed across ${matchCount} matched records</li>
            `;
        }

        strategyInfo.innerHTML = `
            <div class="strategy-title">⚙️ ${filterApplied ? 'API-Based Comparison with BigQuery Filtering' : 'API-Based Comparison'}</div>
            <div class="strategy-description">${strategyDescription}</div>
            <ul class="insight-list">
                ${insightsList}
            </ul>
        `;
    }

    const comparisonSummary = document.getElementById('api-comparison-summary');
    if (comparisonSummary) {
        let summaryCards = `
            <div class="summary-card">
                <div class="summary-number">${uniqueCount}</div>
                <div class="summary-label">Unique Source Records</div>
            </div>
            <div class="summary-card matches">
                <div class="summary-number">${matchCount}</div>
                <div class="summary-label">${filterApplied ? 'Found in Filtered Target' : 'Found in Target'}</div>
            </div>
            <div class="summary-card missing">
                <div class="summary-number">${failedCount}</div>
                <div class="summary-label">${filterApplied ? 'Not in Filtered Target' : 'Not in Target'}</div>
            </div>
            <div class="summary-card differences">
                <div class="summary-number">${summary.totalFieldIssues || 0}</div>
                <div class="summary-label">Field Quality Issues</div>
            </div>
            <div class="summary-card ${parseFloat(successRate) >= 80 ? 'matches' : 'warning'}">
                <div class="summary-number">${successRate}%</div>
                <div class="summary-label">${filterApplied ? 'Filtered Pipeline Success' : 'Pipeline Success Rate'}</div>
            </div>
        `;

        // Add filter reduction card if applicable
        if (filterApplied && filterReduction) {
            summaryCards += `
                <div class="summary-card differences">
                    <div class="summary-number">${filterReduction.percentageReduced}%</div>
                    <div class="summary-label">BigQuery Reduction</div>
                </div>
            `;
        }

        comparisonSummary.innerHTML = summaryCards;
    }

    const comparisonDetails = document.getElementById('api-comparison-details');
    if (comparisonDetails) {
        let detailsContent = '';

        if (filterApplied) {
            // Enhanced details with filter context
            detailsContent = `
                <div style="background: #e8f4fd; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">🔍 Filtered BigQuery Pipeline Validation Analysis</h4>
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; margin-bottom: 15px;">
                        <h5 style="color: #2c3e50; margin-bottom: 8px;">Applied Filter:</h5>
                        <code style="background: #fff; padding: 8px; border-radius: 4px; font-size: 0.9rem; display: block;">${filterCondition}</code>
                    </div>
                    ${originalTargetCount ? `
                        <div style="margin-bottom: 15px;">
                            <h5 style="color: #2c3e50; margin-bottom: 8px;">Filter Impact:</h5>
                            <ul style="margin: 0; padding-left: 20px;">
                                <li>Original BigQuery records: <strong>${originalTargetCount.toLocaleString()}</strong></li>
                                <li>After filtering: <strong>${targetCount.toLocaleString()}</strong></li>
                                <li>Reduction: <strong>${filterReduction?.recordsRemoved?.toLocaleString() || 0}</strong> records (${filterReduction?.percentageReduced || 0}%)</li>
                            </ul>
                        </div>
                    ` : ''}
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                        <div>
                            <h5 style="color: #27ae60; margin-bottom: 10px;">✅ Successfully Transferred to Filtered Target</h5>
                            <p><strong>Count:</strong> ${matchCount} out of ${uniqueCount} unique records</p>
                            <p><strong>Success Rate:</strong> ${successRate}%</p>
                            <p style="font-style: italic; color: #666;">Records found in filtered BigQuery data</p>
                        </div>
                        <div>
                            <h5 style="color: #e74c3c; margin-bottom: 10px;">❌ Not Found in Filtered Target</h5>
                            <p><strong>Count:</strong> ${failedCount} records didn't match filter criteria</p>
                            <p><strong>Impact:</strong> ${failedCount > 0 ? 'API data extends beyond filter scope' : 'Perfect filtered data alignment'}</p>
                            <p style="font-style: italic; color: #666;">May exist in unfiltered BigQuery data</p>
                        </div>
                    </div>
                </div>
            `;
        } else {
            // Standard details (no filter)
            detailsContent = `
                <div style="background: #f0f8ff; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                    <h4 style="color: #2c3e50; margin-bottom: 15px;">📊 API Pipeline Validation Analysis</h4>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                        <div>
                            <h5 style="color: #27ae60; margin-bottom: 10px;">✅ Successfully Transferred Records</h5>
                            <p><strong>Count:</strong> ${matchCount} out of ${uniqueCount} unique records</p>
                            <p><strong>Success Rate:</strong> ${successRate}%</p>
                        </div>
                        <div>
                            <h5 style="color: #e74c3c; margin-bottom: 10px;">❌ Failed to Transfer Records</h5>
                            <p><strong>Count:</strong> ${failedCount} records didn't reach target</p>
                            <p><strong>Impact:</strong> ${failedCount > 0 ? 'API pipeline has gaps - investigation needed' : 'Perfect data transfer'}</p>
                        </div>
                    </div>
                </div>
            `;
        }

        comparisonDetails.innerHTML = detailsContent;
    }
}
        // DIRECT API FIELD ANALYSIS DISPLAY - Replace populateApiFieldWiseTab function
        function populateApiFieldWiseTab(fieldWiseAnalysis, summary) {
            if (!fieldWiseAnalysis || !fieldWiseAnalysis.fieldComparison) {
                const fieldWiseSummary = document.getElementById('api-field-wise-summary');
                if (fieldWiseSummary) {
                    fieldWiseSummary.innerHTML = '<div class="summary-card warning"><div class="summary-number">❌</div><div class="summary-label">Field Analysis Not Available</div></div>';
                }
                return;
            }

            const fieldComparisons = fieldWiseAnalysis.fieldComparison;
            const totalFields = fieldWiseAnalysis.fieldsAnalyzed || 0;
            const perfectFields = fieldWiseAnalysis.perfectFields || 0;
            const problematicFields = fieldWiseAnalysis.problematicFields || 0;
            const totalIssues = fieldWiseAnalysis.totalFieldIssues || 0;
            const recordsAnalyzed = fieldWiseAnalysis.recordsAnalyzed || 0;

            const fieldWiseSummary = document.getElementById('api-field-wise-summary');
            if (fieldWiseSummary) {
                fieldWiseSummary.innerHTML = `
                    <div class="summary-card">
                        <div class="summary-number">${totalFields}</div>
                        <div class="summary-label">Fields Analyzed</div>
                    </div>
                    <div class="summary-card matches">
                        <div class="summary-number">${perfectFields}</div>
                        <div class="summary-label">Perfect Fields</div>
                    </div>
                    <div class="summary-card ${problematicFields > 0 ? 'warning' : 'pass'}">
                        <div class="summary-number">${problematicFields}</div>
                        <div class="summary-label">Fields with Issues</div>
                    </div>
                    <div class="summary-card ${totalIssues > 0 ? 'missing' : 'matches'}">
                        <div class="summary-number">${totalIssues}</div>
                        <div class="summary-label">Total Quality Issues</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-number">${recordsAnalyzed}</div>
                        <div class="summary-label">Records Analyzed</div>
                    </div>
                `;
            }

            const fieldWiseDetails = document.getElementById('api-field-wise-details');
            if (fieldWiseDetails) {
                if (fieldComparisons.length === 0) {
                    fieldWiseDetails.innerHTML = '<p>No field comparisons performed. This usually means no matching records were found to analyze.</p>';
                    return;
                }

                let detailHTML = `
                    <div style="background: #f0f8ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                        <h4 style="color: #2c3e50; margin-bottom: 10px;">🔬 API Field-by-Field Analysis with Sample Records</h4>
                        <p>Detailed comparison of ${totalFields} common columns for ${recordsAnalyzed} matched records. Sample data matches and mismatches are displayed directly below each field.</p>
                    </div>
                    <div class="field-comparison-grid">
                `;

                fieldComparisons.forEach(field => {
                    const cardClass = field.error ? 'warning' : field.differences === 0 ? 'perfect' : 'issues';
                    const statusClass = field.error ? 'warning' : field.differences === 0 ? 'perfect' : 'issues';
                    const statusText = field.error ? '⚠️ Error' : field.differences === 0 ? '✅ Perfect' : `❌ ${field.differences} Issues`;

                    detailHTML += `
                        <div class="field-comparison-card ${cardClass}">
                            <div class="field-header">
                                <div class="field-name">🔗 ${field.fieldName}</div>
                                <div class="field-status ${statusClass}">${statusText}</div>
                            </div>
                            <div class="field-metrics">
                                <div class="field-metric">
                                    <div class="field-metric-number">${field.totalRecords || 0}</div>
                                    <div class="field-metric-label">Records</div>
                                </div>
                                <div class="field-metric">
                                    <div class="field-metric-number" style="color: #27ae60;">${field.perfectMatches || 0}</div>
                                    <div class="field-metric-label">Matches</div>
                                </div>
                                <div class="field-metric">
                                    <div class="field-metric-number" style="color: #e74c3c;">${field.differences || 0}</div>
                                    <div class="field-metric-label">Differences</div>
                                </div>
                                <div class="field-metric">
                                    <div class="field-metric-number">${field.matchRate || '0.0'}%</div>
                                    <div class="field-metric-label">Quality Rate</div>
                                </div>
                            </div>

                            ${generateDirectSampleRecordsDisplay(field, 'API')}
                        </div>
                    `;
                });

                detailHTML += `</div>`;
                fieldWiseDetails.innerHTML = detailHTML;
            }
        }

        // ENHANCED SAMPLE RECORDS DISPLAY - Replace the existing generateSampleRecordsDisplay function
        function generateSampleRecordsDisplay(field, sourceType) {
            if (!field.allComparisons || field.allComparisons.length === 0) {
                return `
                    <div style="margin-top: 15px; padding: 12px; background: #f8f9fa; border-radius: 6px; text-align: center; color: #6c757d; font-style: italic; border: 2px dashed #dee2e6;">
                        📋 No sample records available for this field
                    </div>
                `;
            }

            const matches = field.allComparisons.filter(r => r.comparison_result === 'MATCH').slice(0, 5);
            const differs = field.allComparisons.filter(r => r.comparison_result === 'DIFFER').slice(0, 5);
            const fieldId = field.fieldName.replace(/[^a-zA-Z0-9]/g, '_');

            let sampleHTML = `
                <div class="field-samples">
                    <button
                        class="toggle-samples-btn"
                        onclick="toggleSampleRecords('${fieldId}', this)"
                        style="background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); color: white; border: none; padding: 8px 15px; border-radius: 6px; font-size: 0.8rem; font-weight: 600; cursor: pointer; box-shadow: 0 2px 6px rgba(52, 152, 219, 0.3);"
                    >
                        📊 View Sample Records (${field.allComparisons.length})
                    </button>

                    <div id="samples_${fieldId}" style="display: none; margin-top: 15px;">
            `;

            // Enhanced Sample Data Comparison Table (matching screenshot)
            if (matches.length > 0 || differs.length > 0) {
                const allSamples = [...matches, ...differs].slice(0, 8); // Show up to 8 total records

                sampleHTML += `
                    <div style="background: #f8fff8; border-radius: 12px; padding: 20px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                        <h4 style="color: #2c3e50; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;">
                            📊 Sample Data Comparison
                        </h4>
                        <div style="overflow-x: auto; background: white; border-radius: 8px; border: 1px solid #e9ecef;">
                            <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
                                <thead>
                                    <tr style="background: #f8f9fa;">
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">RECORD KEY</th>
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">${sourceType.toUpperCase()} VALUE</th>
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">BIGQUERY VALUE</th>
                                        <th style="padding: 12px; text-align: center; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">STATUS</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;

                allSamples.forEach((record, index) => {
                    const jsonValue = record.json_value || 'NULL';
                    const bqValue = record.bq_value || 'NULL';
                    const keyValue = record.record_key || 'Unknown';
                    const isMatch = record.comparison_result === 'MATCH';
                    const rowColor = index % 2 === 0 ? '#ffffff' : '#f8f9fa';

                    sampleHTML += `
                        <tr style="background: ${rowColor};">
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: #2c3e50; font-weight: 600; border-bottom: 1px solid #e9ecef;">${keyValue}</td>
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: ${isMatch ? '#155724' : '#721c24'}; border-bottom: 1px solid #e9ecef;">${jsonValue}</td>
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: ${isMatch ? '#155724' : '#721c24'}; border-bottom: 1px solid #e9ecef;">${bqValue}</td>
                            <td style="padding: 10px 12px; text-align: center; border-bottom: 1px solid #e9ecef;">
                                ${isMatch ?
                                    '<span style="color: #27ae60; font-weight: 600; background: #d4edda; padding: 4px 8px; border-radius: 12px; font-size: 0.75rem;">✅ MATCH</span>' :
                                    '<span style="color: #e74c3c; font-weight: 600; background: #f8d7da; padding: 4px 8px; border-radius: 12px; font-size: 0.75rem;">❌ DIFFER</span>'
                                }
                            </td>
                        </tr>
                    `;
                });

                sampleHTML += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }

            // Enhanced Sample Differences Found Section (matching screenshot)
            if (differs.length > 0) {
                sampleHTML += `
                    <div style="background: #fff8e1; border-radius: 12px; padding: 20px; margin-bottom: 15px; border-left: 4px solid #ff9800;">
                        <h4 style="color: #e65100; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;">
                            ⚠️ Sample Differences Found
                        </h4>
                `;

                differs.slice(0, 3).forEach((record, index) => {
                    const jsonValue = record.json_value || 'NULL';
                    const bqValue = record.bq_value || 'NULL';
                    const keyValue = record.record_key || 'Unknown';

                    sampleHTML += `
                        <div style="background: #fff3e0; border: 1px solid #ffcc02; border-radius: 8px; padding: 15px; margin-bottom: ${index < differs.slice(0, 3).length - 1 ? '12px' : '0'}; font-family: monospace; font-size: 0.85rem;">
                            <div style="margin-bottom: 8px;"><strong style="color: #e65100;">Key:</strong> <span style="color: #2c3e50;">${keyValue}</span></div>
                            <div style="margin-bottom: 8px;"><strong style="color: #e65100;">${sourceType}:</strong> <span style="color: #d84315;">${jsonValue}</span></div>
                            <div><strong style="color: #e65100;">BigQuery:</strong> <span style="color: #d84315;">${bqValue}</span></div>
                        </div>
                    `;
                });

                sampleHTML += `</div>`;
            }

            // Perfect field celebration
            if (field.differences === 0 && field.perfectMatches > 0) {
                sampleHTML += `
                    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%); border-radius: 12px; color: #1b5e20; font-weight: 600; margin: 15px 0; border: 2px solid #4caf50;">
                        🎉 Perfect Field Quality - All ${field.totalRecords} records match exactly!
                        <div style="font-size: 0.85rem; margin-top: 8px; font-weight: normal; opacity: 0.9;">100% data integrity between ${sourceType} and BigQuery</div>
                    </div>
                `;
            }

            sampleHTML += `
                    </div>
                </div>
            `;

            return sampleHTML;
        }

        function generateDirectSampleRecordsDisplay(field, sourceType) {
            if (!field.allComparisons || field.allComparisons.length === 0) {
                return `
                    <div style="margin-top: 15px; padding: 12px; background: #f8f9fa; border-radius: 6px; text-align: center; color: #6c757d; font-style: italic; border: 2px dashed #dee2e6;">
                        📋 No sample records available for this field
                    </div>
                `;
            }

            const matches = field.allComparisons.filter(r => r.comparison_result === 'MATCH').slice(0, 5);
            const differs = field.allComparisons.filter(r => r.comparison_result === 'DIFFER').slice(0, 5);

            let sampleHTML = `<div style="margin-top: 15px;">`;

            // Always show Sample Data Comparison Table
            if (matches.length > 0 || differs.length > 0) {
                const allSamples = [...matches, ...differs].slice(0, 10); // Show up to 10 total records

                sampleHTML += `
                    <div style="background: #f8fff8; border-radius: 12px; padding: 20px; margin-bottom: 20px; border-left: 4px solid #3498db;">
                        <h4 style="color: #2c3e50; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;">
                            📊 Sample Data Comparison
                        </h4>
                        <div style="overflow-x: auto; background: white; border-radius: 8px; border: 1px solid #e9ecef;">
                            <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
                                <thead>
                                    <tr style="background: #f8f9fa;">
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">RECORD KEY</th>
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">${sourceType.toUpperCase()} VALUE</th>
                                        <th style="padding: 12px; text-align: left; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">BIGQUERY VALUE</th>
                                        <th style="padding: 12px; text-align: center; font-weight: 600; color: #2c3e50; border-bottom: 2px solid #dee2e6;">STATUS</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;

                allSamples.forEach((record, index) => {
                    const jsonValue = record.json_value || 'NULL';
                    const bqValue = record.bq_value || 'NULL';
                    const keyValue = record.record_key || 'Unknown';
                    const isMatch = record.comparison_result === 'MATCH';
                    const rowColor = index % 2 === 0 ? '#ffffff' : '#f8f9fa';

                    sampleHTML += `
                        <tr style="background: ${rowColor};">
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: #2c3e50; font-weight: 600; border-bottom: 1px solid #e9ecef;">${keyValue}</td>
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: ${isMatch ? '#155724' : '#721c24'}; border-bottom: 1px solid #e9ecef;">${jsonValue}</td>
                            <td style="padding: 10px 12px; font-family: monospace; font-size: 0.8rem; color: ${isMatch ? '#155724' : '#721c24'}; border-bottom: 1px solid #e9ecef;">${bqValue}</td>
                            <td style="padding: 10px 12px; text-align: center; border-bottom: 1px solid #e9ecef;">
                                ${isMatch ?
                                    '<span style="color: #27ae60; font-weight: 600; background: #d4edda; padding: 4px 8px; border-radius: 12px; font-size: 0.75rem;">✅ MATCH</span>' :
                                    '<span style="color: #e74c3c; font-weight: 600; background: #f8d7da; padding: 4px 8px; border-radius: 12px; font-size: 0.75rem;">❌ DIFFER</span>'
                                }
                            </td>
                        </tr>
                    `;
                });

                sampleHTML += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }

            // Always show Sample Differences Found section if there are differences
            if (differs.length > 0) {
                sampleHTML += `
                    <div style="background: #fff8e1; border-radius: 12px; padding: 20px; margin-bottom: 15px; border-left: 4px solid #ff9800;">
                        <h4 style="color: #e65100; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;">
                            ⚠️ Sample Differences Found
                        </h4>
                `;

                differs.slice(0, 3).forEach((record, index) => {
                    const jsonValue = record.json_value || 'NULL';
                    const bqValue = record.bq_value || 'NULL';
                    const keyValue = record.record_key || 'Unknown';

                    sampleHTML += `
                        <div style="background: #fff3e0; border: 1px solid #ffcc02; border-radius: 8px; padding: 15px; margin-bottom: ${index < differs.slice(0, 3).length - 1 ? '12px' : '0'}; font-family: monospace; font-size: 0.85rem;">
                            <div style="margin-bottom: 8px;"><strong style="color: #e65100;">Key:</strong> <span style="color: #2c3e50;">${keyValue}</span></div>
                            <div style="margin-bottom: 8px;"><strong style="color: #e65100;">${sourceType}:</strong> <span style="color: #d84315;">${jsonValue}</span></div>
                            <div><strong style="color: #e65100;">BigQuery:</strong> <span style="color: #d84315;">${bqValue}</span></div>
                        </div>
                    `;
                });

                sampleHTML += `</div>`;
            }

            // Perfect field celebration
            if (field.differences === 0 && field.perfectMatches > 0) {
                sampleHTML += `
                    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%); border-radius: 12px; color: #1b5e20; font-weight: 600; margin: 15px 0; border: 2px solid #4caf50;">
                        🎉 Perfect Field Quality - All ${field.totalRecords} records match exactly!
                        <div style="font-size: 0.85rem; margin-top: 8px; font-weight: normal; opacity: 0.9;">100% data integrity between ${sourceType} and BigQuery</div>
                    </div>
                `;
            }

            sampleHTML += `</div>`;
            return sampleHTML;
        }

        // FIXED Toggle function for sample records
        function toggleSampleRecords(fieldId, buttonElement) {
            const samplesDiv = document.getElementById(`samples_${fieldId}`);

            if (samplesDiv && buttonElement) {
                if (samplesDiv.style.display === 'none') {
                    samplesDiv.style.display = 'block';
                    buttonElement.innerHTML = '📋 Hide Sample Records';
                    buttonElement.style.background = 'linear-gradient(135deg, #95a5a6 0%, #7f8c8d 100%)';

                    // Smooth scroll to show the opened section
                    setTimeout(() => {
                        samplesDiv.scrollIntoView({
                            behavior: 'smooth',
                            block: 'nearest'
                        });
                    }, 100);

                } else {
                    samplesDiv.style.display = 'none';
                    const totalSamples = samplesDiv.querySelectorAll('table tbody tr').length;
                    buttonElement.innerHTML = `📊 View Sample Records (${totalSamples > 0 ? totalSamples : 'Available'})`;
                    buttonElement.style.background = 'linear-gradient(135deg, #3498db 0%, #2980b9 100%)';
                }
            }
        }

        function populateApiDuplicatesTab(duplicatesAnalysis, summary) {
            if (!duplicatesAnalysis) {
                const duplicatesSummary = document.getElementById('api-duplicates-summary');
                if (duplicatesSummary) {
                    duplicatesSummary.innerHTML = '<div class="summary-card warning"><div class="summary-number">❌</div><div class="summary-label">Analysis Failed</div></div>';
                }
                return;
            }

            const apiDuplicates = duplicatesAnalysis.apiDuplicates || { duplicateCount: 0, totalDuplicateRecords: 0 };
            const bqDuplicates = duplicatesAnalysis.bqDuplicates || { duplicateCount: 0, totalDuplicateRecords: 0 };
            const crossSystem = duplicatesAnalysis.crossSystemAnalysis || {};
            const duplicatesSummaryData = duplicatesAnalysis.summary || {};

            const duplicatesSummary = document.getElementById('api-duplicates-summary');
            if (duplicatesSummary) {
                duplicatesSummary.innerHTML = `
                    <div class="summary-card ${apiDuplicates.duplicateCount > 0 ? 'warning' : 'pass'}">
                        <div class="summary-number">${apiDuplicates.duplicateCount}</div>
                        <div class="summary-label">API Duplicate Keys</div>
                    </div>
                    <div class="summary-card ${bqDuplicates.duplicateCount > 0 ? 'warning' : 'pass'}">
                        <div class="summary-number">${bqDuplicates.duplicateCount}</div>
                        <div class="summary-label">BigQuery Duplicate Keys</div>
                    </div>
                    <div class="summary-card ${crossSystem.commonDuplicateKeys?.length > 0 ? 'missing' : 'pass'}">
                        <div class="summary-number">${crossSystem.commonDuplicateKeys?.length || 0}</div>
                        <div class="summary-label">Common Duplicates</div>
                    </div>
                    <div class="summary-card ${duplicatesSummaryData.bothSystemsClean ? 'matches' : 'warning'}">
                        <div class="summary-number">${duplicatesSummaryData.dataQualityScore || 'Unknown'}</div>
                        <div class="summary-label">Data Quality Score</div>
                    </div>
                    <div class="summary-card">
                        <div class="summary-number">${(apiDuplicates.totalDuplicateRecords || 0) + (bqDuplicates.totalDuplicateRecords || 0)}</div>
                        <div class="summary-label">Total Duplicate Records</div>
                    </div>
                `;
            }

            const duplicatesDetails = document.getElementById('api-duplicates-details');
            if (duplicatesDetails) {
                if (duplicatesSummaryData.bothSystemsClean) {
                    duplicatesDetails.innerHTML = `
                        <div style="text-align: center; padding: 40px; background: #d4edda; border-radius: 8px; color: #155724;">
                            <h3 style="margin-bottom: 15px;">✅ Excellent Data Quality in Both Systems!</h3>
                            <p style="margin-bottom: 10px;">Both API source and BigQuery target have clean, unique primary key values.</p>
                            <p style="font-style: italic;">This is ideal for data processing and ensures accurate comparisons across systems.</p>
                        </div>
                    `;
                    return;
                }

                let detailHTML = `
                    <div style="background: #f0f8ff; padding: 20px; border-radius: 8px; margin-bottom: 25px; border-left: 4px solid #3498db;">
                        <h4 style="color: #2c3e50; margin-bottom: 15px;">🔍 API-BigQuery Duplicates Analysis</h4>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 25px;">
                            <div>
                                <h5 style="color: #3498db; margin-bottom: 12px;">🔗 API Source System</h5>
                                <p><strong>Duplicate Keys:</strong> ${apiDuplicates.duplicateCount}</p>
                                <p><strong>Affected Records:</strong> ${apiDuplicates.totalDuplicateRecords}</p>
                                <p><strong>Status:</strong> <span style="color: ${apiDuplicates.duplicateCount === 0 ? '#27ae60' : '#e74c3c'};">${apiDuplicates.duplicateCount === 0 ? '✅ Clean' : '⚠️ Has Duplicates'}</span></p>
                            </div>
                            <div>
                                <h5 style="color: #2980b9; margin-bottom: 12px;">🗄️ BigQuery Target System</h5>
                                <p><strong>Duplicate Keys:</strong> ${bqDuplicates.duplicateCount}</p>
                                <p><strong>Affected Records:</strong> ${bqDuplicates.totalDuplicateRecords}</p>
                                <p><strong>Status:</strong> <span style="color: ${bqDuplicates.duplicateCount === 0 ? '#27ae60' : '#e74c3c'};">${bqDuplicates.duplicateCount === 0 ? '✅ Clean' : '⚠️ Has Duplicates'}</span></p>
                            </div>
                        </div>
                    </div>
                `;

                if (crossSystem.commonDuplicateKeys && crossSystem.commonDuplicateKeys.length > 0) {
                    detailHTML += `
                        <div style="background: #fff3cd; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #ffc107;">
                            <h5 style="color: #856404; margin-bottom: 15px;">🚨 Critical: Duplicates Found in BOTH Systems</h5>
                            <p style="margin-bottom: 15px;"><strong>Impact:</strong> ${crossSystem.commonDuplicateKeys.length} duplicate key(s) exist in both API and BigQuery systems.</p>
                            <div style="background: white; padding: 15px; border-radius: 6px;">
                                <h6 style="margin-bottom: 10px;">Common Duplicate Keys:</h6>
                                <div style="font-family: monospace; font-size: 0.9rem;">
                                    ${crossSystem.commonDuplicateKeys.slice(0, 10).map(key =>
                                        `<span style="background: #f8d7da; color: #721c24; padding: 3px 8px; margin: 2px; border-radius: 4px; display: inline-block;">${key}</span>`
                                    ).join(' ')}
                                </div>
                            </div>
                        </div>
                    `;
                }

                duplicatesDetails.innerHTML = detailHTML;
            }
        }

        // Enhanced Excel Export for API Results
        // Replace the exportApiResultsToExcel function in index.html with this enhanced version
// that includes BigQuery filter information in the Excel report

function exportApiResultsToExcel() {
    if (!globalApiResults) {
        alert('No API comparison results available for export. Please run an API comparison first.');
        return;
    }

    try {
        console.log('Starting comprehensive API vs BQ Excel export with BigQuery filter information...');

        const wb = XLSX.utils.book_new();

        // Get filter information from results
        const filterApplied = globalApiResults.summary?.bigQueryFilterApplied || false;
        const filterCondition = globalApiResults.summary?.bigQueryFilterCondition || null;
        const originalBqRecords = globalApiResults.summary?.originalBigQueryRecords || null;
        const filteredBqRecords = globalApiResults.summary?.targetRecords || 0;
        const filterReduction = globalApiResults.summary?.filterReduction || null;

        // Enhanced Sheet 1: Executive Summary with Filter Information
        const summaryData = [
            ['API vs BigQuery ETL Validation Report', '', '', '', ''],
            ['Generated:', new Date().toLocaleString(), '', '', ''],
            ['Primary Key Used:', globalApiResults.primaryKeyUsed || 'N/A', '', '', ''],
            ['API Endpoint:', globalApiResults.metadata?.url || 'N/A', '', '', ''],
            ['Authentication Type:', globalApiResults.metadata?.authType || 'N/A', '', '', ''],
            ['BigQuery Filter Applied:', filterApplied ? 'Yes' : 'No', '', '', ''],
            ...(filterApplied ? [
                ['Filter Condition:', filterCondition || 'N/A', '', '', ''],
                ...(originalBqRecords ? [
                    ['Original BigQuery Records:', originalBqRecords.toLocaleString(), '', '', ''],
                    ['Filtered BigQuery Records:', filteredBqRecords.toLocaleString(), '', '', ''],
                    ['Filter Reduction:', `${filterReduction?.recordsRemoved?.toLocaleString() || 0} records (${filterReduction?.percentageReduced || 0}%)`, '', '', '']
                ] : [])
            ] : []),
            ['', '', '', '', ''],
            ['EXECUTIVE SUMMARY', '', '', '', ''],
            ['Metric', 'Value', 'Status', 'Impact', 'Notes'],
            ['Pipeline Success Rate', (globalApiResults.summary?.pipelineSuccessRate || '0') + '%',
             parseFloat(globalApiResults.summary?.pipelineSuccessRate || 0) >= 90 ? 'Excellent' :
             parseFloat(globalApiResults.summary?.pipelineSuccessRate || 0) >= 70 ? 'Good' : 'Needs Attention',
             'Business Critical',
             filterApplied ? 'API to Filtered BQ transfer rate' : 'API to BQ transfer rate'],

            ['Total API Records', globalApiResults.summary?.totalRecordsInFile || 0, 'Info', 'Volume', 'From API response'],

            ['Records Reached Target', globalApiResults.summary?.recordsReachedTarget || 0, 'Info', 'Success',
             filterApplied ? 'Found in filtered BigQuery data' : 'Found in BigQuery'],

            ['Failed to Reach Target', globalApiResults.summary?.recordsFailedToReachTarget || 0,
             (globalApiResults.summary?.recordsFailedToReachTarget || 0) === 0 ? 'Perfect' : 'Review Required',
             'Data Quality',
             filterApplied ? 'Not matching filter criteria' : 'Missing from BQ'],

            ['Schema Compatibility', globalApiResults.summary?.schemaCompatibility || 'N/A', 'Info', 'Integration', 'Field matching %'],

            ['Field Quality Issues', globalApiResults.summary?.totalFieldIssues || 0,
             (globalApiResults.summary?.totalFieldIssues || 0) === 0 ? 'Perfect' : 'Review Required',
             'Data Integrity',
             filterApplied ? 'Value mismatches in filtered data' : 'Value mismatches'],

            ['API Authentication Status', globalApiResults.metadata?.authenticationStatus || 'Unknown', 'Info', 'Security', 'Auth validation'],
            ['API Response Time', (globalApiResults.metadata?.responseTime || 'N/A') + 'ms', 'Info', 'Performance', 'API latency'],

            // Add filter-specific metrics if applied
            ...(filterApplied ? [
                ['', '', '', '', ''],
                ['BIGQUERY FILTER ANALYSIS', '', '', '', ''],
                ['Filter Applied', 'Yes', 'Info', 'Data Scope', 'BigQuery data filtered before comparison'],
                ['Filter Condition', filterCondition || 'N/A', 'Info', 'SQL Logic', 'Applied WHERE clause'],
                ...(originalBqRecords ? [
                    ['Original BQ Records', originalBqRecords.toLocaleString(), 'Info', 'Full Dataset', 'Before filtering'],
                    ['Filtered BQ Records', filteredBqRecords.toLocaleString(), 'Info', 'Filtered Dataset', 'After applying filter'],
                    ['Filter Effectiveness', `${filterReduction?.percentageReduced || 0}% reduction`,
                     parseFloat(filterReduction?.percentageReduced || 0) > 50 ? 'High Impact' :
                     parseFloat(filterReduction?.percentageReduced || 0) > 20 ? 'Moderate Impact' : 'Low Impact',
                     'Filter Performance', 'Records removed by filter']
                ] : [])
            ] : [])
        ];

        const summarySheet = XLSX.utils.aoa_to_sheet(summaryData);
        XLSX.utils.book_append_sheet(wb, summarySheet, 'Executive Summary');

        // Enhanced Sheet 2: Filter Analysis (if filter was applied)
        if (filterApplied) {
            const filterAnalysisData = [
                ['BigQuery Filter Analysis Report', '', '', ''],
                ['Generated:', new Date().toLocaleString(), '', ''],
                ['Filter Condition:', filterCondition || 'N/A', '', ''],
                ['', '', '', ''],
                ['FILTER IMPACT ANALYSIS', '', '', ''],
                ['Metric', 'Value', 'Impact', 'Notes'],
                ['Original BigQuery Records', originalBqRecords?.toLocaleString() || 'Unknown', 'Baseline', 'Full dataset before filtering'],
                ['Records After Filter', filteredBqRecords.toLocaleString(), 'Filtered Dataset', 'Records matching filter criteria'],
                ['Records Removed', filterReduction?.recordsRemoved?.toLocaleString() || 'Unknown', 'Filter Effect', 'Records excluded by filter'],
                ['Reduction Percentage', `${filterReduction?.percentageReduced || 0}%`, 'Filter Efficiency', 'Percentage of data filtered out'],
                ['', '', '', ''],
                ['COMPARISON RESULTS WITH FILTER', '', '', ''],
                ['API Records Found in Filtered BQ', globalApiResults.summary?.recordsReachedTarget || 0, 'Match Success', 'API records matching filter criteria'],
                ['API Records Not in Filtered BQ', globalApiResults.summary?.recordsFailedToReachTarget || 0, 'Filter Mismatch', 'May exist in unfiltered data'],
                ['Pipeline Success Rate', `${globalApiResults.summary?.pipelineSuccessRate || 0}%`, 'Filter-Based Success', 'Success rate with filtered target'],
                ['', '', '', ''],
                ['RECOMMENDATIONS', '', '', ''],
                ['Recommendation', 'Rationale', 'Priority', 'Action'],
                ...(filterReduction?.percentageReduced > 50 ? [
                    ['Review Filter Criteria', 'High filter reduction detected', 'High', 'Ensure filter is not overly restrictive']
                ] : []),
                ...(globalApiResults.summary?.recordsFailedToReachTarget > 0 ? [
                    ['Check Unfiltered Data', 'Some API records not in filtered BQ', 'Medium', 'Verify if missing records exist in full BigQuery table']
                ] : []),
                ['Validate Filter Logic', 'Ensure filter matches intended business logic', 'Medium', 'Review filter condition with business stakeholders'],
                ['Monitor Filter Performance', 'Track filter effectiveness over time', 'Low', 'Regular review of filter criteria relevance']
            ];

            const filterSheet = XLSX.utils.aoa_to_sheet(filterAnalysisData);
            XLSX.utils.book_append_sheet(wb, filterSheet, 'Filter Analysis');
        }

        // Enhanced Sheet 3: Record Count Analysis with Filter Context
        if (globalApiResults.recordCounts) {
            const recordCountData = [
                ['Record Count Analysis', '', '', ''],
                ['Report Type:', filterApplied ? 'API vs Filtered BigQuery' : 'API vs BigQuery', '', ''],
                ['Generated:', new Date().toLocaleString(), '', ''],
                ...(filterApplied ? [
                    ['Filter Applied:', 'Yes', '', ''],
                    ['Filter Condition:', filterCondition || 'N/A', '', '']
                ] : [
                    ['Filter Applied:', 'No', '', '']
                ]),
                ['', '', '', ''],
                ['SOURCE ANALYSIS (API)', '', '', ''],
                ['Metric', 'Count', 'Percentage', 'Notes'],
                ['Total Records', globalApiResults.recordCounts.apiDetails?.totalRecords || globalApiResults.recordCounts.jsonDetails?.totalRecords || 0, '100%', 'All API records'],
                ['Unique Primary Keys', globalApiResults.recordCounts.apiDetails?.uniquePrimaryKeys || globalApiResults.recordCounts.jsonDetails?.uniquePrimaryKeys || 0,
                 `${((globalApiResults.recordCounts.apiDetails?.uniquePrimaryKeys || globalApiResults.recordCounts.jsonDetails?.uniquePrimaryKeys || 0) / Math.max(globalApiResults.recordCounts.apiDetails?.totalRecords || globalApiResults.recordCounts.jsonDetails?.totalRecords || 1, 1) * 100).toFixed(1)}%`,
                 'Distinct key values'],
                ['Duplicate Records', globalApiResults.recordCounts.apiDetails?.duplicateRecords || globalApiResults.recordCounts.jsonDetails?.duplicateRecords || 0,
                 `${((globalApiResults.recordCounts.apiDetails?.duplicateRecords || globalApiResults.recordCounts.jsonDetails?.duplicateRecords || 0) / Math.max(globalApiResults.recordCounts.apiDetails?.totalRecords || globalApiResults.recordCounts.jsonDetails?.totalRecords || 1, 1) * 100).toFixed(1)}%`,
                 'Records with duplicate keys'],
                ['', '', '', ''],
                [`TARGET ANALYSIS (${filterApplied ? 'Filtered BigQuery' : 'BigQuery'})`, '', '', ''],
                ['Metric', 'Count', 'Percentage', 'Notes'],
                ['Total Records', globalApiResults.recordCounts.bqDetails?.totalRecords || 0, '100%',
                 filterApplied ? 'Records matching filter criteria' : 'All BigQuery records'],
                ['Unique Primary Keys', globalApiResults.recordCounts.bqDetails?.uniquePrimaryKeys || 0,
                 `${((globalApiResults.recordCounts.bqDetails?.uniquePrimaryKeys || 0) / Math.max(globalApiResults.recordCounts.bqDetails?.totalRecords || 1, 1) * 100).toFixed(1)}%`,
                 'Distinct key values in target'],
                ...(filterApplied && originalBqRecords ? [
                    ['', '', '', ''],
                    ['ORIGINAL BIGQUERY (Pre-Filter)', '', '', ''],
                    ['Original Total Records', originalBqRecords.toLocaleString(), '100%', 'Full BigQuery dataset'],
                    ['Filter Reduction', filterReduction?.recordsRemoved?.toLocaleString() || 0, `${filterReduction?.percentageReduced || 0}%`, 'Records removed by filter']
                ] : [])
            ];

            const recordCountSheet = XLSX.utils.aoa_to_sheet(recordCountData);
            XLSX.utils.book_append_sheet(wb, recordCountSheet, 'Record Counts');
        }

        // NEW: Sheet 4 - Field-by-Field Analysis
        const fieldWiseAnalysis = globalApiResults.fieldWiseAnalysis || {};
        const fields = fieldWiseAnalysis.fieldResults || fieldWiseAnalysis.fieldComparison || [];
        
        console.log('Field-wise analysis data for export:', {
            hasFieldWiseAnalysis: !!globalApiResults.fieldWiseAnalysis,
            fieldsCount: fields.length,
            fieldsAnalyzed: fieldWiseAnalysis.fieldsAnalyzed,
            perfectFields: fieldWiseAnalysis.perfectFields,
            problematicFields: fieldWiseAnalysis.problematicFields
        });
        
        // Always create Field Analysis sheet (even if empty, show why)
        const fieldAnalysisData = [
            ['FIELD-BY-FIELD ANALYSIS REPORT'],
            ['Generated:', new Date().toLocaleString()],
            ['Primary Key:', globalApiResults.summary?.primaryKeyUsed || globalApiResults.metadata?.primaryKey || 'N/A'],
            ['Total Fields Analyzed:', fieldWiseAnalysis.fieldsAnalyzed || fields.length || 0],
            ['Records Analyzed:', fieldWiseAnalysis.recordsAnalyzed || 0],
            [''],
            ['FIELD COMPARISON SUMMARY'],
            ['Field Name', 'Total Records', 'Matches', 'Mismatches', 'Match Rate', 'Status']
        ];
        
        let perfectFieldCount = 0;
        let problematicFieldCount = 0;
        
        if (fields.length > 0) {
            fields.forEach(f => {
                const fieldName = f.fieldName || f.field || 'Unknown';
                const totalRecords = f.totalRecords || 0;
                const matches = f.matchCount || f.matches || f.perfectMatches || 0;
                const mismatches = f.mismatchCount || f.mismatches || f.differences || 0;
                const matchRate = f.matchRate || f.matchPercentage || 
                                 (totalRecords > 0 ? ((matches / totalRecords) * 100).toFixed(1) : '100');
                const status = parseFloat(matchRate) >= 100 ? '✓ Perfect' : 
                              parseFloat(matchRate) >= 95 ? '⚠ Minor Issues' : '✗ Needs Review';
                
                if (parseFloat(matchRate) >= 100) {
                    perfectFieldCount++;
                } else {
                    problematicFieldCount++;
                }
                
                fieldAnalysisData.push([fieldName, totalRecords, matches, mismatches, matchRate + '%', status]);
            });
        } else {
            // No field data available - add explanation
            fieldAnalysisData.push(['No field analysis data available', '', '', '', '', '']);
            fieldAnalysisData.push(['', '', '', '', '', '']);
            fieldAnalysisData.push(['Possible reasons:', '', '', '', '', '']);
            fieldAnalysisData.push(['- No matching records found between API and BigQuery', '', '', '', '', '']);
            fieldAnalysisData.push(['- Field analysis was not performed during comparison', '', '', '', '', '']);
            fieldAnalysisData.push(['- Primary key mismatch prevented record matching', '', '', '', '', '']);
        }
        
        // Add summary at the end
        fieldAnalysisData.push(['']);
        fieldAnalysisData.push(['SUMMARY']);
        fieldAnalysisData.push(['Perfect Fields (100% match):', perfectFieldCount]);
        fieldAnalysisData.push(['Fields with Issues:', problematicFieldCount]);
        fieldAnalysisData.push(['Overall Field Quality:', fields.length > 0 && perfectFieldCount === fields.length ? 'Excellent' : (fields.length === 0 ? 'No Data' : 'Needs Review')]);
        
        const fieldAnalysisSheet = XLSX.utils.aoa_to_sheet(fieldAnalysisData);
        fieldAnalysisSheet['!cols'] = [{wch: 45}, {wch: 15}, {wch: 12}, {wch: 12}, {wch: 12}, {wch: 15}];
        XLSX.utils.book_append_sheet(wb, fieldAnalysisSheet, 'Field Analysis');

        // NEW: Sheet 5 - Field Mismatches Detail (sample differences)
        const mismatchDetailData = [
            ['FIELD MISMATCH DETAILS'],
            ['Shows sample records where API and BigQuery values differ'],
            [''],
            ['Field Name', 'Primary Key', 'API Value', 'BigQuery Value']
        ];
        
        let hasMismatches = false;
        
        if (fields.length > 0) {
            fields.forEach(f => {
                const fieldName = f.fieldName || f.field || 'Unknown';
                const sampleDifferences = f.sampleDifferences || f.differenceSamples || [];
                
                if (sampleDifferences.length > 0) {
                    hasMismatches = true;
                    sampleDifferences.slice(0, 10).forEach(d => {
                        mismatchDetailData.push([
                            fieldName,
                            d.primaryKey || d.key || 'N/A',
                            String(d.apiValue || d.jsonValue || d.sourceValue || ''),
                            String(d.bqValue || d.targetValue || '')
                        ]);
                    });
                }
            });
        }
        
        if (!hasMismatches) {
            mismatchDetailData.push(['No mismatches found - all field values match perfectly!', '', '', '']);
        }
        
        const mismatchSheet = XLSX.utils.aoa_to_sheet(mismatchDetailData);
        mismatchSheet['!cols'] = [{wch: 30}, {wch: 40}, {wch: 40}, {wch: 40}];
        XLSX.utils.book_append_sheet(wb, mismatchSheet, 'Mismatch Details');

        // NEW: Sheet 6 - Sample Matched Records
        const matchedSamplesData = [
            ['SAMPLE MATCHED RECORDS'],
            ['Shows sample records where API and BigQuery values are identical'],
            [''],
            ['Field Name', 'Primary Key', 'Matched Value']
        ];
        
        let hasMatches = false;
        
        if (fields.length > 0) {
            fields.forEach(f => {
                const fieldName = f.fieldName || f.field || 'Unknown';
                const sampleMatches = f.sampleMatches || f.matchedSamples || [];
                
                if (sampleMatches.length > 0) {
                    hasMatches = true;
                    sampleMatches.slice(0, 5).forEach(m => {
                        matchedSamplesData.push([
                            fieldName,
                            m.primaryKey || m.key || 'N/A',
                            String(m.value || m.apiValue || m.matchedValue || '')
                        ]);
                    });
                }
            });
        }
        
        if (!hasMatches) {
            matchedSamplesData.push(['No sample matches available', '', '']);
        }
        
        const matchedSheet = XLSX.utils.aoa_to_sheet(matchedSamplesData);
        matchedSheet['!cols'] = [{wch: 30}, {wch: 40}, {wch: 50}];
        XLSX.utils.book_append_sheet(wb, matchedSheet, 'Matched Samples');

        // NEW: Sheet 7 - Schema Analysis
        const schemaAnalysis = globalApiResults.schemaAnalysis || {};
        const schemaData = [
            ['SCHEMA ANALYSIS REPORT'],
            ['Comparison of fields between API response and BigQuery table'],
            [''],
            ['COMMON FIELDS (exist in both)', 'API-ONLY FIELDS', 'BIGQUERY-ONLY FIELDS']
        ];
        
        const commonFields = schemaAnalysis.commonFields || [];
        const apiOnlyFields = schemaAnalysis.jsonOnlyFields || schemaAnalysis.tempOnlyFields || [];
        const bqOnlyFields = schemaAnalysis.bqOnlyFields || schemaAnalysis.sourceOnlyFields || [];
        
        if (commonFields.length > 0 || apiOnlyFields.length > 0 || bqOnlyFields.length > 0) {
            const maxRows = Math.max(commonFields.length, apiOnlyFields.length, bqOnlyFields.length);
            
            for (let i = 0; i < maxRows; i++) {
                schemaData.push([
                    commonFields[i] || '',
                    apiOnlyFields[i] || '',
                    bqOnlyFields[i] || ''
                ]);
            }
        } else {
            schemaData.push(['No schema analysis data available', '', '']);
        }
        
        schemaData.push(['']);
        schemaData.push(['SUMMARY']);
        schemaData.push(['Common Fields:', commonFields.length]);
        schemaData.push(['API-Only Fields:', apiOnlyFields.length]);
        schemaData.push(['BigQuery-Only Fields:', bqOnlyFields.length]);
        schemaData.push(['Schema Compatibility:', commonFields.length > 0 ? `${((commonFields.length / Math.max(commonFields.length + apiOnlyFields.length, 1)) * 100).toFixed(1)}%` : 'N/A']);
        
        const schemaSheet = XLSX.utils.aoa_to_sheet(schemaData);
        schemaSheet['!cols'] = [{wch: 40}, {wch: 40}, {wch: 40}];
        XLSX.utils.book_append_sheet(wb, schemaSheet, 'Schema Analysis');

        // NEW: Sheet 8 - Duplicates Analysis
        const duplicatesAnalysis = globalApiResults.duplicatesAnalysis || {};
        const duplicatesData = [
            ['DUPLICATES ANALYSIS REPORT'],
            ['Analysis of duplicate primary keys in API and BigQuery data'],
            [''],
            ['SOURCE', 'METRIC', 'VALUE']
        ];
        
        // API Duplicates
        const apiDuplicates = duplicatesAnalysis.apiDuplicates || duplicatesAnalysis.jsonDuplicates || {};
        duplicatesData.push(['API Data', 'Total Duplicate Keys', apiDuplicates.count || apiDuplicates.duplicateCount || 0]);
        
        if (apiDuplicates.sampleKeys && apiDuplicates.sampleKeys.length > 0) {
            duplicatesData.push(['API Data', 'Sample Duplicate Keys', apiDuplicates.sampleKeys.slice(0, 10).join(', ')]);
        }
        
        // BQ Duplicates
        const bqDuplicates = duplicatesAnalysis.bqDuplicates || {};
        duplicatesData.push(['BigQuery Data', 'Total Duplicate Keys', bqDuplicates.count || bqDuplicates.duplicateCount || 0]);
        
        if (bqDuplicates.sampleKeys && bqDuplicates.sampleKeys.length > 0) {
            duplicatesData.push(['BigQuery Data', 'Sample Duplicate Keys', bqDuplicates.sampleKeys.slice(0, 10).join(', ')]);
        }
        
        duplicatesData.push(['']);
        duplicatesData.push(['SUMMARY']);
        duplicatesData.push(['Total API Duplicates:', apiDuplicates.count || apiDuplicates.duplicateCount || 0]);
        duplicatesData.push(['Total BQ Duplicates:', bqDuplicates.count || bqDuplicates.duplicateCount || 0]);
        duplicatesData.push(['Data Quality:', (apiDuplicates.count || 0) === 0 && (bqDuplicates.count || 0) === 0 ? 'Excellent - No Duplicates' : 'Review Required - Duplicates Found']);
        
        const duplicatesSheet = XLSX.utils.aoa_to_sheet(duplicatesData);
        duplicatesSheet['!cols'] = [{wch: 20}, {wch: 25}, {wch: 60}];
        XLSX.utils.book_append_sheet(wb, duplicatesSheet, 'Duplicates Analysis');

        // Generate filename with filter context
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T');
        const filterSuffix = filterApplied ? '_Filtered' : '';
        const filename = `API_vs_BQ${filterSuffix}_Validation_Report_${timestamp[0]}_${timestamp[1].split('.')[0]}.xlsx`;

        XLSX.writeFile(wb, filename);

        console.log(`Enhanced API vs BQ Excel report exported: ${filename}`);

        // Success feedback with filter context
        const exportButton = document.getElementById('exportApiToExcel');
        const originalText = exportButton.innerHTML;
        const successMessage = filterApplied ? '✅ Filtered Report Exported!' : '✅ Exported Successfully!';

        exportButton.innerHTML = successMessage;
        exportButton.style.background = '#27ae60';

        setTimeout(() => {
            exportButton.innerHTML = originalText;
            exportButton.style.background = '';
        }, 3000);

    } catch (error) {
        console.error('Enhanced API Excel export failed:', error);
        alert('Excel export failed: ' + error.message);
    }
}
        // ===========================================
        // JSON vs BQ RESULTS FUNCTIONALITY
        // ===========================================


