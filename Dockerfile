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

# Create config directory
RUN mkdir -p /app/config

# Expose port (Cloud Run uses PORT env variable)
EXPOSE 8080

# Start the application
CMD ["npm", "start"]