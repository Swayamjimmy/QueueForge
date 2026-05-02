FROM node:22-alpine

WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY package*.json ./
RUN npm install --production

# Copy application source code
COPY . .

EXPOSE 3000

CMD ["node", "server.js"]