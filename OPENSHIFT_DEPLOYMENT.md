# OpenShift Deployment Guide

This guide explains how to deploy the Cloudant Data Extraction Pipeline to an OpenShift cluster.

## 🎯 Overview

The application has been adapted for OpenShift with the following key changes:

- **Non-privileged port**: Uses port 8080 instead of port 80
- **Non-root user support**: Container runs with arbitrary UIDs as required by OpenShift
- **Proper file permissions**: All directories have group write permissions
- **OpenShift-native resources**: Route, BuildConfig, ImageStream support

## 📋 Prerequisites

- OpenShift cluster access (4.x or later)
- `oc` CLI tool installed and configured
- Cloudant credentials (username, password, URL)
- Sufficient cluster resources:
  - 2 CPU cores
  - 2GB RAM
  - 12GB persistent storage

## 🚀 Quick Deployment

### Option 1: Using OpenShift CLI (Recommended)

1. **Login to your OpenShift cluster:**
```bash
oc login https://your-openshift-cluster:6443
```

2. **Create a new project:**
```bash
oc new-project cloudant-extractor
```

3. **Update credentials in secret file:**
```bash
# Edit openshift/secret.yaml with your actual Cloudant credentials
vi openshift/secret.yaml
```

4. **Deploy all resources:**
```bash
# Create secret
oc apply -f openshift/secret.yaml

# Create persistent volume claims
oc apply -f openshift/pvc.yaml

# Build the image (if using BuildConfig)
oc apply -f openshift/buildconfig.yaml

# Or import pre-built image
oc import-image cloudant-extractor:latest --from=image-registry.openshift-image-registry.svc:5000/cloudant-extractor/cloudant-extractor:latest --confirm

# Deploy the application
oc apply -f openshift/deployment.yaml

# Create service
oc apply -f openshift/service.yaml

# Create route
oc apply -f openshift/route.yaml
```

5. **Check deployment status:**
```bash
oc get pods
oc get route
```

6. **Access the application:**
```bash
# Get the route URL
oc get route cloudant-extractor -o jsonpath='{.spec.host}'

# Open in browser
https://<route-url>
```

### Option 2: Using OpenShift Web Console

1. **Login to OpenShift Web Console**

2. **Create New Project:**
   - Click "Create Project"
   - Name: `cloudant-extractor`

3. **Import YAML:**
   - Click "+Add" → "Import YAML"
   - Copy and paste contents of each YAML file in this order:
     1. `openshift/secret.yaml` (update credentials first!)
     2. `openshift/pvc.yaml`
     3. `openshift/buildconfig.yaml` (optional, if building from source)
     4. `openshift/deployment.yaml`
     5. `openshift/service.yaml`
     6. `openshift/route.yaml`

4. **Monitor Deployment:**
   - Go to "Topology" view
   - Wait for pod to be running (green circle)

5. **Access Application:**
   - Click on the route icon in topology view
   - Or go to "Networking" → "Routes"

## 🔧 Configuration

### Environment Variables

Edit [`openshift/deployment.yaml`](openshift/deployment.yaml) to modify:

```yaml
env:
- name: BATCH_SIZE
  value: "1000"          # Records per batch
- name: START_YEAR
  value: "2024"          # Starting year
- name: START_MONTH
  value: "1"             # Starting month (1-12)
- name: END_YEAR
  value: "2026"          # Ending year
- name: END_MONTH
  value: "12"            # Ending month (1-12)
```

### Resource Limits

Adjust resource requests/limits in [`openshift/deployment.yaml`](openshift/deployment.yaml):

```yaml
resources:
  requests:
    memory: "512Mi"      # Minimum memory
    cpu: "250m"          # Minimum CPU
  limits:
    memory: "2Gi"        # Maximum memory
    cpu: "1000m"         # Maximum CPU
```

### Storage Size

Modify storage size in [`openshift/pvc.yaml`](openshift/pvc.yaml):

```yaml
resources:
  requests:
    storage: 10Gi        # Adjust based on expected data volume
```

## 🔒 Security Configuration

### Using Secrets

**Never commit actual credentials!** Update the secret before deploying:

```bash
# Create secret from command line (more secure)
oc create secret generic cloudant-credentials \
  --from-literal=username='your_username' \
  --from-literal=password='your_password' \
  --from-literal=url='https://your-instance.cloudant.com/db/_design/view/_view/name'
```

Or use OpenShift's secret management:

```bash
# Create secret from file
oc create secret generic cloudant-credentials \
  --from-file=username=./username.txt \
  --from-file=password=./password.txt \
  --from-file=url=./url.txt
```

### Network Policies

For enhanced security, create a NetworkPolicy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: cloudant-extractor-netpol
spec:
  podSelector:
    matchLabels:
      app: cloudant-extractor
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: openshift-ingress
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: TCP
      port: 443  # HTTPS to Cloudant
```

## 🏗️ Building from Source

### Using OpenShift BuildConfig

The [`openshift/buildconfig.yaml`](openshift/buildconfig.yaml) enables building directly in OpenShift:

1. **Update Git repository URL:**
```yaml
source:
  type: Git
  git:
    uri: https://github.com/YOUR-ORG/YOUR-REPO.git
    ref: main
```

2. **Start build:**
```bash
oc start-build cloudant-extractor
```

3. **Monitor build:**
```bash
oc logs -f bc/cloudant-extractor
```

### Building Locally and Pushing

```bash
# Build OpenShift-compatible image
podman build -f Dockerfile.openshift -t cloudant-extractor:latest .

# Tag for your registry
podman tag cloudant-extractor:latest your-registry/cloudant-extractor:latest

# Push to registry
podman push your-registry/cloudant-extractor:latest

# Update deployment to use your image
oc set image deployment/cloudant-extractor cloudant-extractor=your-registry/cloudant-extractor:latest
```

## 📊 Monitoring

### View Logs

```bash
# Application logs
oc logs -f deployment/cloudant-extractor

# Specific container logs
oc logs -f deployment/cloudant-extractor -c cloudant-extractor

# Previous pod logs (if crashed)
oc logs deployment/cloudant-extractor --previous
```

### Check Pod Status

```bash
# List pods
oc get pods

# Describe pod
oc describe pod <pod-name>

# Get pod events
oc get events --sort-by='.lastTimestamp'
```

### Access Pod Shell

```bash
# Open shell in running pod
oc rsh deployment/cloudant-extractor

# Execute command
oc exec deployment/cloudant-extractor -- ls -la /app/backend/extractions/
```

### Health Checks

The deployment includes liveness and readiness probes:

```bash
# Check health endpoint
oc exec deployment/cloudant-extractor -- curl http://localhost:8080/api/health
```

## 💾 Data Management

### Access Extracted Files

```bash
# List extraction files
oc exec deployment/cloudant-extractor -- ls -lh /app/backend/extractions/

# Copy file from pod
oc cp <pod-name>:/app/backend/extractions/extraction_20260324_120000.json ./extraction.json

# View file size
oc exec deployment/cloudant-extractor -- du -sh /app/backend/extractions/
```

### Backup Data

```bash
# Create backup of extraction data
oc exec deployment/cloudant-extractor -- tar -czf /tmp/backup.tar.gz /app/backend/extractions/

# Copy backup to local machine
oc cp <pod-name>:/tmp/backup.tar.gz ./backup.tar.gz
```

### Persistent Volume Management

```bash
# List PVCs
oc get pvc

# Describe PVC
oc describe pvc cloudant-extraction-data

# Check PV usage
oc exec deployment/cloudant-extractor -- df -h /app/backend/extractions/
```

## 🔄 Updates and Maintenance

### Update Application

```bash
# Pull latest code
git pull

# Rebuild image
oc start-build cloudant-extractor

# Or update deployment with new image
oc set image deployment/cloudant-extractor cloudant-extractor=your-registry/cloudant-extractor:v2

# Rollout status
oc rollout status deployment/cloudant-extractor
```

### Rollback Deployment

```bash
# View rollout history
oc rollout history deployment/cloudant-extractor

# Rollback to previous version
oc rollout undo deployment/cloudant-extractor

# Rollback to specific revision
oc rollout undo deployment/cloudant-extractor --to-revision=2
```

### Scale Application

```bash
# Scale to multiple replicas (if needed)
oc scale deployment/cloudant-extractor --replicas=2

# Autoscaling (optional)
oc autoscale deployment/cloudant-extractor --min=1 --max=3 --cpu-percent=80
```

## 🐛 Troubleshooting

### Pod Not Starting

```bash
# Check pod status
oc get pods
oc describe pod <pod-name>

# Check events
oc get events --sort-by='.lastTimestamp' | grep cloudant-extractor

# Check logs
oc logs <pod-name>
```

### Image Pull Errors

```bash
# Check image stream
oc get imagestream

# Check build logs
oc logs -f bc/cloudant-extractor

# Verify image exists
oc describe is/cloudant-extractor
```

### Permission Issues

OpenShift runs containers with random UIDs. If you see permission errors:

```bash
# Check pod's UID
oc exec deployment/cloudant-extractor -- id

# Verify directory permissions
oc exec deployment/cloudant-extractor -- ls -la /app/backend/extractions/
```

The [`Dockerfile.openshift`](Dockerfile.openshift) already handles this with proper group permissions.

### Connection Issues

```bash
# Test Cloudant connectivity from pod
oc exec deployment/cloudant-extractor -- curl -v https://your-instance.cloudant.com

# Check DNS resolution
oc exec deployment/cloudant-extractor -- nslookup your-instance.cloudant.com

# Verify secrets are mounted
oc exec deployment/cloudant-extractor -- env | grep CLOUDANT
```

### Route Not Working

```bash
# Check route
oc get route cloudant-extractor

# Describe route
oc describe route cloudant-extractor

# Test from within cluster
oc run test-pod --image=curlimages/curl --rm -it -- curl http://cloudant-extractor:8080/api/health
```

## 🔐 Production Best Practices

### 1. Use Separate Namespaces

```bash
# Development
oc new-project cloudant-extractor-dev

# Production
oc new-project cloudant-extractor-prod
```

### 2. Resource Quotas

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: cloudant-quota
spec:
  hard:
    requests.cpu: "2"
    requests.memory: 4Gi
    persistentvolumeclaims: "3"
```

### 3. Limit Ranges

```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: cloudant-limits
spec:
  limits:
  - max:
      memory: 2Gi
      cpu: "1"
    min:
      memory: 256Mi
      cpu: 100m
    type: Container
```

### 4. Network Policies

Restrict network access to only necessary services.

### 5. RBAC

Create service accounts with minimal required permissions:

```bash
oc create serviceaccount cloudant-extractor-sa
oc adm policy add-role-to-user view system:serviceaccount:cloudant-extractor:cloudant-extractor-sa
```

### 6. Monitoring and Alerts

Integrate with OpenShift monitoring:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cloudant-extractor-metrics
  labels:
    app: cloudant-extractor
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
spec:
  ports:
  - name: metrics
    port: 8080
  selector:
    app: cloudant-extractor
```

## 📈 Performance Tuning

### Optimize for Large Datasets

1. **Increase resources:**
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

2. **Adjust batch size:**
```yaml
env:
- name: BATCH_SIZE
  value: "5000"  # Increase for faster extraction
```

3. **Use faster storage class:**
```yaml
storageClassName: fast-ssd  # If available in your cluster
```

## 🆘 Support

For issues specific to OpenShift deployment:

1. Check OpenShift logs: `oc logs -f deployment/cloudant-extractor`
2. Review pod events: `oc describe pod <pod-name>`
3. Verify resource availability: `oc describe node`
4. Check application logs in the pod

For application-specific issues, refer to the main [README.md](README.md).

---

## 📝 Summary

Your application is now OpenShift-ready with:

- ✅ Non-privileged port (8080)
- ✅ Non-root user support
- ✅ Proper file permissions for arbitrary UIDs
- ✅ OpenShift-native resources (Route, BuildConfig, ImageStream)
- ✅ Persistent storage for data
- ✅ Health checks and monitoring
- ✅ Secure credential management
- ✅ Production-ready configuration

Deploy with confidence! 🚀