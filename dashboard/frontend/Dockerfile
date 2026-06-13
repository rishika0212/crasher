FROM node:18-alpine

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

EXPOSE 5173

# Bind to 0.0.0.0 to allow routing from other docker containers
CMD ["npm", "run", "dev", "--", "--host"]
