#!/bin/bash
# Startup script for Cloudant Data Extraction Pipeline Container

set -e

echo "=========================================="
echo "Cloudant Data Extraction Pipeline"
echo "Starting container services..."
echo "=========================================="

# Check if .env file exists
if [ ! -f /app/.env ]; then
    echo "WARNING: .env file not found!"
    echo "Please ensure environment variables are set:"
    echo "  - CLOUDANT_USERNAME"
    echo "  - CLOUDANT_PASSWORD"
    echo "  - CLOUDANT_URL"
fi

# Create necessary directories
mkdir -p /app/backend/extractions
mkdir -p /var/log/supervisor

echo "✓ Directories created"

# Start supervisor (manages nginx and flask)
echo "✓ Starting services..."
echo "  - Nginx (port 80) - Frontend & API proxy"
echo "  - Flask (port 5000) - Backend API"
echo "=========================================="

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

# Made with Bob
