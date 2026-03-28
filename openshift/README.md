# OpenShift Deployment Resources

This directory contains all the necessary Kubernetes/OpenShift manifests to deploy the Cloudant Data Extraction Pipeline to an OpenShift cluster.

## 📁 Files Overview

| File | Description |
|------|-------------|
| [`deployment.yaml`](deployment.yaml) | Main application deployment with container specs, env vars, health checks |
| [`service.yaml`](service.yaml) | ClusterIP service exposing port 8080 |
| [`route.yaml`](route.yaml) | OpenShift Route for external access with TLS |
| [`pvc.yaml`](pvc.yaml) | Persistent Volume Claims for data storage (10Gi + 2x1Gi) |
| [`secret.yaml`](secret.yaml) | Secret for Cloudant credentials (UPDATE BEFORE DEPLOYING!) |
| [`buildconfig.yaml`](buildconfig.yaml) | OpenShift BuildConfig for building from source |
| [`kustomization.yaml`](kustomization.yaml) | Kustomize configuration for easy deployment |

## 🚀 Quick Deploy

### Using Kustomize (Recommended)

```bash
# Login to OpenShift
oc login https://your-cluster:6443

# Create project
oc new-project cloudant-extractor

# Update credentials in secret.yaml first!
vi secret.yaml

# Deploy everything
oc apply -k .
```

### Using Individual Files

```bash
# Deploy in order
oc apply -f secret.yaml
oc apply -f pvc.yaml
oc apply -f buildconfig.yaml
oc apply -f deployment.yaml
oc apply -f service.yaml
oc apply -f route.yaml
```

## ⚙️ Configuration

### Before Deploying

1. **Update [`secret.yaml`](secret.yaml)** with your Cloudant credentials:
   ```yaml
   stringData:
     username: "YOUR_ACTUAL_USERNAME"
     password: "YOUR_ACTUAL_PASSWORD"
     url: "https://your-instance.cloudant.com/db/_design/view/_view/name"
   ```

2. **Adjust resources in [`deployment.yaml`](deployment.yaml)** if needed:
   ```yaml
   resources:
     requests:
       memory: "512Mi"
       cpu: "250m"
     limits:
       memory: "2Gi"
       cpu: "1000m"
   ```

3. **Modify storage size in [`pvc.yaml`](pvc.yaml)** based on expected data volume:
   ```yaml
   resources:
     requests:
       storage: 10Gi  # Adjust as needed
   ```

### After Deploying

```bash
# Get route URL
oc get route cloudant-extractor -o jsonpath='{.spec.host}'

# Check pod status
oc get pods

# View logs
oc logs -f deployment/cloudant-extractor
```

## 📚 Full Documentation

See [OPENSHIFT_DEPLOYMENT.md](../OPENSHIFT_DEPLOYMENT.md) for:
- Detailed deployment instructions
- Troubleshooting guide
- Monitoring and maintenance
- Security best practices
- Performance tuning

## 🔒 Security Notes

- **Never commit actual credentials** to version control
- The `secret.yaml` file contains placeholder values
- Use `oc create secret` for production deployments
- Consider using OpenShift's secret management or external secret stores

## 🆘 Quick Troubleshooting

```bash
# Pod not starting?
oc describe pod <pod-name>
oc logs <pod-name>

# Can't access application?
oc get route
oc get svc

# Storage issues?
oc get pvc
oc describe pvc cloudant-extraction-data
```

For more help, see the [full troubleshooting guide](../OPENSHIFT_DEPLOYMENT.md#-troubleshooting).