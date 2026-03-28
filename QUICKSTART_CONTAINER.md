# Quick Start - Container Deployment

Run the Cloudant Data Extraction Pipeline in a container with just 3 steps!

## Prerequisites

- Podman installed ([Installation Guide](CONTAINER.md#installing-podman))
- Cloudant credentials

## 3-Step Setup

### 1. Create Configuration File

```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```bash
CLOUDANT_USERNAME=your_username
CLOUDANT_PASSWORD=your_password
CLOUDANT_URL=https://your-instance.cloudant.com/db/_design/view/_view/name
```

### 2. Run the Container

```bash
./run-podman.sh
```

### 3. Access the Application

Open your browser: **http://localhost:8080**

## That's It! 🎉

The application is now running with:
- ✅ React frontend on port 8080
- ✅ Flask backend API
- ✅ Persistent data storage

## Common Commands

```bash
# View logs
podman logs -f cloudant-extractor

# Stop container
podman stop cloudant-extractor

# Restart container
podman restart cloudant-extractor

# Remove container
podman rm -f cloudant-extractor
```

## Troubleshooting

**Container won't start?**
```bash
# Check logs
podman logs cloudant-extractor

# Verify .env file exists
cat .env
```

**Port 8080 already in use?**

Edit `run-podman.sh` and change the port:
```bash
-p 9090:80 \  # Change 8080 to 9090
```

## Next Steps

- See [CONTAINER.md](CONTAINER.md) for detailed documentation
- See [README.md](README.md) for application features
- See [SETUP.md](SETUP.md) for non-container setup

## Need Help?

Check the full container guide: [CONTAINER.md](CONTAINER.md)