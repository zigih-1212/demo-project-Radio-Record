FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json server.ts ./
COPY src ./src
COPY scripts ./scripts
EXPOSE 3000
CMD ["npm", "start"]
