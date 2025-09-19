FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .

# Make sure the public directory exists
RUN mkdir -p /app/public /app/uploads /app/temp-files

# List files to debug (remove this line after fixing)
RUN ls -la /app && ls -la /app/public || echo "public directory not found"

EXPOSE 8080
CMD ["node", "server.js"]