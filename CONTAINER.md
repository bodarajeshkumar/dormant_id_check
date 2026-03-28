# Container Deployment Guide

This guide explains how to run the Cloudant Data Extraction Pipeline in a container using Podman (or Docker).

## 🐳 Container Architecture

The application runs in a **single container** with:
- **Nginx** (port 80) - Serves React frontend and proxies API requests
- **Flask** (port 5000) - Backend API server
- **Supervisor** - Process manager for both services

## 📋 Prerequisites

- Podman installed (or Docker as an alternative)
- Cloudant credentials (username, password, URL)

### Installing Podman

**macOS:**
```bash
brew install podman
podman machine init
podman machine start
```

**Linux:**
```bash
# Debian/Ubuntu
sudo apt-get install podman

# RHEL/CentOS/Fedora
sudo dnf install podman
```

**Windows:**
Download from [Podman Desktop](https://podman-desktop.io/)

## 🚀 Quick Start

### Option 1: Using the Run Script (Easiest)

1. **Create `.env` file** with your credentials:
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

2. **Run the script:**
```bash
./run-podman.sh
```

That's it! The script will:
- Build the container image
- Start the container with all necessary configuration
- Display access URLs and useful commands

3. **Access the application:**
   - Open browser: http://localhost:8080
   - API endpoint: http://localhost:8080/api/status

4. **View logs:**
```bash
podman logs -f cloudant-extractor
```

5. **Stop the container:**
```bash
podman stop cloudant-extractor
```

### Option 2: Using Podman Compose (If Installed)

1. **Create `.env` file** with your credentials:
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

2. **Start the container:**
```bash
podman-compose up -d
```

3. **Access the application:**
   - Open browser: http://localhost:8080
   - API endpoint: http://localhost:8080/api/status

4. **View logs:**
```bash
podman-compose logs -f
```

5. **Stop the container:**
```bash
podman-compose down
```

**Note:** If you don't have `podman-compose` installed, use Option 1 or Option 3 instead.

### Option 3: Using Podman CLI Directly

1. **Build the image:**
```bash
podman build -t cloudant-extractor:latest .
```

2. **Run the container:**
```bash
podman run -d \
  --name cloudant-extractor \
  -p 8080:80 \
  -e CLOUDANT_USERNAME="your_username" \
  -e CLOUDANT_PASSWORD="your_password" \
  -e CLOUDANT_URL="https://your-instance.cloudant.com/db/_design/view/_view/name" \
  -v extraction-data:/app/backend/extractions \
  cloudant-extractor:latest
```

3. **Access the application:**
   - Open browser: http://localhost:8080

4. **View logs:**
```bash
podman logs -f cloudant-extractor
```

5. **Stop and remove:**
```bash
podman stop cloudant-extractor
podman rm cloudant-extractor
```

## 🔧 Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLOUDANT_USERNAME` | Yes | - | Cloudant username |
| `CLOUDANT_PASSWORD` | Yes | - | Cloudant password |
| `CLOUDANT_URL` | Yes | - | Full Cloudant view URL |
| `BATCH_SIZE` | No | 1000 | Records per batch |
| `START_YEAR` | No | 2024 | Starting year for extraction |
| `START_MONTH` | No | 1 | Starting month (1-12) |
| `END_YEAR` | No | 2026 | Ending year for extraction |
| `END_MONTH` | No | 12 | Ending month (1-12) |

### Volume Mounts

The container uses volumes to persist data:

- **`/app/backend/extractions`** - Extracted JSON files
- **`/app/status.json`** - Current job status
- **`/app/extraction_history.json`** - Extraction history

## 📊 Monitoring

### Health Check

The container includes a health check endpoint:
```bash
curl http://localhost:8080/api/health
```

### Check Container Status

```bash
# Using podman-compose
podman-compose ps

# Using podman CLI
podman ps
podman inspect cloudant-extractor
```

### View Service Logs

```bash
# All logs
podman logs cloudant-extractor

# Follow logs
podman logs -f cloudant-extractor

# Last 100 lines
podman logs --tail 100 cloudant-extractor

# Nginx logs
podman exec cloudant-extractor cat /var/log/supervisor/nginx.log

# Flask logs
podman exec cloudant-extractor cat /var/log/supervisor/flask.log
```

## 🔍 Troubleshooting

### Container Won't Start

1. **Check logs:**
```bash
podman logs cloudant-extractor
```

2. **Verify environment variables:**
```bash
podman exec cloudant-extractor env | grep CLOUDANT
```

3. **Check if port is already in use:**
```bash
# Change port mapping if needed
podman run -p 9090:80 ...
```

### Cannot Access Application

1. **Verify container is running:**
```bash
podman ps | grep cloudant-extractor
```

2. **Check port mapping:**
```bash
podman port cloudant-extractor
```

3. **Test health endpoint:**
```bash
curl http://localhost:8080/api/health
```

### Extraction Fails

1. **Check credentials:**
```bash
podman exec cloudant-extractor env | grep CLOUDANT
```

2. **View Flask logs:**
```bash
podman exec cloudant-extractor cat /var/log/supervisor/flask.log
```

3. **Test Cloudant connection:**
```bash
podman exec -it cloudant-extractor python -c "
import os
import requests
from requests.auth import HTTPBasicAuth

url = os.getenv('CLOUDANT_URL')
username = os.getenv('CLOUDANT_USERNAME')
password = os.getenv('CLOUDANT_PASSWORD')

response = requests.get(url, auth=HTTPBasicAuth(username, password))
print(f'Status: {response.status_code}')
"
```

## 💾 Data Management

### Access Extracted Files

```bash
# List extraction files
podman exec cloudant-extractor ls -lh /app/backend/extractions/

# Copy file from container
podman cp cloudant-extractor:/app/backend/extractions/extraction_20260324_120000.json ./

# View file size
podman exec cloudant-extractor du -sh /app/backend/extractions/
```

### Backup Extraction Data

```bash
# Create backup
podman exec cloudant-extractor tar -czf /tmp/extractions-backup.tar.gz /app/backend/extractions/

# Copy backup to host
podman cp cloudant-extractor:/tmp/extractions-backup.tar.gz ./extractions-backup.tar.gz
```

### Clean Up Old Extractions

```bash
# Remove files older than 7 days
podman exec cloudant-extractor find /app/backend/extractions/ -name "*.json" -mtime +7 -delete
```

## 🔄 Updates and Maintenance

### Update Container

```bash
# Pull latest code
git pull

# Rebuild image
podman build -t cloudant-extractor:latest .

# Stop old container
podman-compose down

# Start new container
podman-compose up -d
```

### Restart Services

```bash
# Restart entire container
podman restart cloudant-extractor

# Restart specific service inside container
podman exec cloudant-extractor supervisorctl restart flask
podman exec cloudant-extractor supervisorctl restart nginx
```

## 🐋 Docker Compatibility

All commands work with Docker by replacing `podman` with `docker`:

```bash
# Build with Docker
docker build -t cloudant-extractor:latest .

# Run with Docker
docker run -d --name cloudant-extractor -p 8080:80 ...

# Use docker-compose
docker-compose up -d
```

## 🔒 Security Best Practices

1. **Never commit `.env` file** - It's in `.gitignore`
2. **Use secrets management** for production:
```bash
# Using Podman secrets
echo "your_password" | podman secret create cloudant_password -
podman run --secret cloudant_password ...
```

3. **Run as non-root** (future enhancement)
4. **Use HTTPS** in production with reverse proxy
5. **Limit container resources:**
```bash
podman run --memory="2g" --cpus="2" ...
```

## 📈 Production Deployment

### Using Podman with Systemd

1. **Generate systemd service:**
```bash
podman generate systemd --new --name cloudant-extractor > /etc/systemd/system/cloudant-extractor.service
```

2. **Enable and start:**
```bash
sudo systemctl enable cloudant-extractor
sudo systemctl start cloudant-extractor
```

### Using Kubernetes/OpenShift

Convert to Kubernetes YAML:
```bash
podman generate kube cloudant-extractor > cloudant-extractor-k8s.yaml
```

## 🆘 Support

For issues or questions:
1. Check logs: `podman logs cloudant-extractor`
2. Review this guide
3. Check main [README.md](README.md) for application details
4. Open an issue on the repository

---

**Note:** This container setup is optimized for development and small-scale production use. For large-scale production deployments, consider:
- Separate containers for frontend and backend
- Load balancing
- Database for status/history instead of JSON files
- Monitoring and alerting (Prometheus, Grafana)
- Automated backups