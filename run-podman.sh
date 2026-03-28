#!/bin/bash
# Script to run Cloudant Extractor with Podman (without podman-compose)

set -e

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "ERROR: .env file not found!"
    echo "Please create .env file with your Cloudant credentials."
    echo "You can copy .env.example and edit it:"
    echo "  cp .env.example .env"
    exit 1
fi

# Check required environment variables
if [ -z "$CLOUDANT_USERNAME" ] || [ -z "$CLOUDANT_PASSWORD" ] || [ -z "$CLOUDANT_URL" ]; then
    echo "ERROR: Missing required environment variables!"
    echo "Please ensure .env file contains:"
    echo "  - CLOUDANT_USERNAME"
    echo "  - CLOUDANT_PASSWORD"
    echo "  - CLOUDANT_URL"
    exit 1
fi

# Set defaults for optional variables
BATCH_SIZE=${BATCH_SIZE:-1000}
START_YEAR=${START_YEAR:-2024}
START_MONTH=${START_MONTH:-1}
END_YEAR=${END_YEAR:-2026}
END_MONTH=${END_MONTH:-12}

echo "=========================================="
echo "Cloudant Data Extraction Pipeline"
echo "=========================================="
echo "Building container image..."

# Build the image
podman build -t cloudant-extractor:latest .

echo "=========================================="
echo "Starting container..."
echo "=========================================="

# Stop and remove existing container if it exists
podman stop cloudant-extractor 2>/dev/null || true
podman rm cloudant-extractor 2>/dev/null || true

# Create volume for extraction data if it doesn't exist
podman volume create extraction-data 2>/dev/null || true

# Create status files if they don't exist
touch status.json 2>/dev/null || true
touch extraction_history.json 2>/dev/null || true

# Run the container
podman run -d \
  --name cloudant-extractor \
  -p 8080:80 \
  -e CLOUDANT_USERNAME="$CLOUDANT_USERNAME" \
  -e CLOUDANT_PASSWORD="$CLOUDANT_PASSWORD" \
  -e CLOUDANT_URL="$CLOUDANT_URL" \
  -e BATCH_SIZE="$BATCH_SIZE" \
  -e START_YEAR="$START_YEAR" \
  -e START_MONTH="$START_MONTH" \
  -e END_YEAR="$END_YEAR" \
  -e END_MONTH="$END_MONTH" \
  -v extraction-data:/app/backend/extractions \
  -v "$(pwd)/status.json:/app/status.json" \
  -v "$(pwd)/extraction_history.json:/app/extraction_history.json" \
  --restart unless-stopped \
  cloudant-extractor:latest

echo "=========================================="
echo "✓ Container started successfully!"
echo "=========================================="
echo ""
echo "Access the application:"
echo "  Frontend: http://localhost:8080"
echo "  API:      http://localhost:8080/api/status"
echo ""
echo "Useful commands:"
echo "  View logs:    podman logs -f cloudant-extractor"
echo "  Stop:         podman stop cloudant-extractor"
echo "  Restart:      podman restart cloudant-extractor"
echo "  Remove:       podman rm -f cloudant-extractor"
echo ""
echo "To view logs now, run:"
echo "  podman logs -f cloudant-extractor"
echo "=========================================="

# Made with Bob
