# Use official Node.js runtime (changed from alpine to slim for Oracle compatibility)
FROM node:18-slim

# Install system dependencies for Oracle Instant Client
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client
RUN mkdir -p /opt/oracle && \
    cd /tmp && \
    wget https://download.oracle.com/otn_software/linux/instantclient/1924000/instantclient-basic-linux.x64-19.24.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-19.24.0.0.0dbru.zip -d /opt/oracle && \
    rm instantclient-basic-linux.x64-19.24.0.0.0dbru.zip

# Set Oracle environment variables
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_24:$LD_LIBRARY_PATH
ENV PATH=/opt/oracle/instantclient_19_24:$PATH

# Set working directory
WORKDIR /app

# Copy package files first (for better caching)
COPY package*.json ./

# Install dependencies
RUN npm install --only=production

# Copy application code
COPY . .

# Create config directory
RUN mkdir -p /app/config

# Expose port (Cloud Run uses PORT env variable)
EXPOSE 8080

# Start the application
CMD ["npm", "start"]