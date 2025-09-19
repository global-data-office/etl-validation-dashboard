# Use official Node.js runtime
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files first (for better caching)
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/config /app/uploads /app/temp-files /app/routes /app/services

# Expose port
EXPOSE 8080

# Start the application directly with node
CMD ["node", "server.js"]