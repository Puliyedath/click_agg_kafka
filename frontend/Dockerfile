FROM node:18-alpine

ENV CI=true

# Set the working directory in the container
WORKDIR /app

COPY frontend/package*.json ./

RUN npm install

COPY frontend/ ./

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "${PORT:-5173}"]

