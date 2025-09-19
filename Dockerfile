# Use official Node.js runtime
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install system dependencies for potential native modules
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    && rm -rf /var/cache/apk/*

# Copy package files first (for better Docker caching)
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production && npm cache clean --force

# Create necessary directories first
RUN mkdir -p \
    /app/public \
    /app/uploads \
    /app/temp-files \
    /app/routes \
    /app/services \
    /app/config

# Copy all application files
COPY . .

# CRITICAL FIX: Move index.html to public directory if it's not there
RUN if [ -f /app/index.html ] && [ ! -f /app/public/index.html ]; then \
        echo "Moving index.html to public directory..." && \
        mv /app/index.html /app/public/index.html; \
    fi

# Verify the file structure and show debug info
RUN echo "=== DOCKER BUILD VERIFICATION ===" && \
    echo "Files in /app:" && ls -la /app/ && \
    echo "=== CHECKING CRITICAL FILES ===" && \
    test -f /app/server.js && echo "✅ server.js found" || echo "❌ server.js missing" && \
    test -f /app/package.json && echo "✅ package.json found" || echo "❌ package.json missing" && \
    test -d /app/public && echo "✅ public directory found" || echo "❌ public directory missing" && \
    test -f /app/public/index.html && echo "✅ index.html found in public/" || echo "❌ index.html missing from public/" && \
    echo "=== PUBLIC DIRECTORY CONTENTS ===" && \
    ls -la /app/public/ && \
    echo "=== ROUTES DIRECTORY CHECK ===" && \
    ls -la /app/routes/ || echo "❌ Routes directory missing" && \
    echo "=== SERVICES DIRECTORY CHECK ===" && \
    ls -la /app/services/ || echo "❌ Services directory missing" && \
    echo "=== BUILD VERIFICATION COMPLETE ==="

# Set proper permissions
RUN chown -R node:node /app
USER node

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD node -e "const http = require('http'); \
    const req = http.request('http://localhost:8080/api/health', (res) => { \
        process.exit(res.statusCode === 200 ? 0 : 1); \
    }); \
    req.on('error', () => process.exit(1)); \
    req.end();"

# Environment variables
ENV NODE_ENV=production
ENV PORT=8080

# Expose port
EXPOSE 8080

# Start the application
CMD ["node", "server.js"]