# Multi-stage Dockerfile for Cloudant Data Extraction Pipeline
# Runs both React frontend and Flask backend in a single container

# Stage 1: Build React frontend
FROM node:18-alpine AS frontend-builder

WORKDIR /frontend

# Copy frontend package files
COPY frontend/package*.json ./

# Install dependencies (use npm install instead of npm ci for flexibility)
RUN npm install --only=production

# Copy frontend source
COPY frontend/ ./

# Set production environment variable for build
ENV REACT_APP_API_URL=/api

# Build the React app for production
RUN npm run build

# Stage 2: Final image with Python backend and built frontend
FROM python:3.11-slim

# Install system dependencies including nginx for serving frontend
RUN apt-get update && apt-get install -y \
    gcc \
    nginx \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python requirements
COPY requirements.txt ./
COPY backend/requirements.txt ./backend-requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r backend-requirements.txt

# Copy backend application
COPY cloudant_extractor.py ./
COPY backend/ ./backend/

# Copy built frontend from builder stage
COPY --from=frontend-builder /frontend/build /usr/share/nginx/html

# Create necessary directories
RUN mkdir -p /app/backend/extractions && \
    mkdir -p /var/log/supervisor

# Configure nginx
RUN echo 'server {\n\
    listen 80;\n\
    server_name localhost;\n\
    \n\
    # Serve React frontend\n\
    location / {\n\
        root /usr/share/nginx/html;\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
    \n\
    # Proxy API requests to Flask backend\n\
    location /api/ {\n\
        proxy_pass http://127.0.0.1:5000/api/;\n\
        proxy_http_version 1.1;\n\
        proxy_set_header Upgrade $http_upgrade;\n\
        proxy_set_header Connection "upgrade";\n\
        proxy_set_header Host $host;\n\
        proxy_set_header X-Real-IP $remote_addr;\n\
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n\
        proxy_set_header X-Forwarded-Proto $scheme;\n\
        proxy_read_timeout 300s;\n\
        proxy_connect_timeout 75s;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf && \
    rm -f /etc/nginx/sites-enabled/default

# Create supervisor configuration
RUN echo '[supervisord]\n\
nodaemon=true\n\
logfile=/var/log/supervisor/supervisord.log\n\
pidfile=/var/run/supervisord.pid\n\
\n\
[program:nginx]\n\
command=/usr/sbin/nginx -g "daemon off;"\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/var/log/supervisor/nginx.log\n\
stderr_logfile=/var/log/supervisor/nginx_err.log\n\
\n\
[program:flask]\n\
command=python /app/backend/app.py\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/var/log/supervisor/flask.log\n\
stderr_logfile=/var/log/supervisor/flask_err.log\n\
environment=PYTHONUNBUFFERED=1\n' > /etc/supervisor/conf.d/supervisord.conf

# Expose port 80 for nginx (serves both frontend and proxies backend)
EXPOSE 80

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost/api/health || exit 1

# Start supervisor to manage both nginx and flask
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]