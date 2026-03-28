# How to Get W3ID OIDC Client ID and Secret

## Overview
To use W3ID OIDC authentication, you need to register your application with IBM's W3ID system to obtain OAuth 2.0 credentials (Client ID and Client Secret).

---

## Method 1: IBM Cloud Identity (Recommended for IBM Employees)

### Step 1: Access IBM Cloud Identity Portal

1. Navigate to: **https://www.ibm.com/cloud/identity**
2. Or go to: **https://cloud.ibm.com/iam/overview**
3. Login with your IBM credentials (w3id)

### Step 2: Create a New Application

1. Go to **"Manage" → "Access (IAM)"**
2. Click on **"Service IDs"** or **"API Keys"**
3. Select **"Create"** to create a new service ID
4. Fill in the application details:
   - **Name**: `Cloudant Data Extraction App`
   - **Description**: `OAuth application for Cloudant data extraction control panel`
   - **Application Type**: `Web Application`

### Step 3: Configure OAuth Settings

1. In the application settings, configure:
   - **Redirect URIs**: 
     ```
     http://localhost:5000/auth/callback (for development)
     https://your-production-domain.com/auth/callback (for production)
     ```
   - **Grant Types**: Select `Authorization Code`
   - **Scopes**: Select `openid`, `profile`, `email`

2. **Save** the configuration

### Step 4: Obtain Credentials

After saving, you'll receive:
- **Client ID**: A unique identifier (e.g., `abc123-def456-ghi789`)
- **Client Secret**: A secret key (keep this secure!)

**Important**: Copy and save these credentials immediately - the secret may not be shown again!

---

## Method 2: IBM W3ID Self-Service Portal

### Step 1: Access W3ID Portal

1. Navigate to: **https://w3.ibm.com/tools/w3id/**
2. Or: **https://w3id.sso.ibm.com/**
3. Login with your IBM w3id credentials

### Step 2: Register Application

1. Look for **"Register Application"** or **"OAuth Applications"**
2. Click **"New Application"** or **"Register"**
3. Fill in the registration form:

   ```
   Application Name: Cloudant Data Extraction Control Panel
   Application Type: Web Application
   Description: Internal tool for extracting and managing Cloudant data
   
   Redirect URIs:
   - http://localhost:5000/auth/callback
   - https://your-app-domain.com/auth/callback
   
   Grant Types:
   ☑ Authorization Code
   ☐ Implicit
   ☐ Client Credentials
   
   Scopes:
   ☑ openid
   ☑ profile  
   ☑ email
   ```

4. Submit the registration

### Step 3: Approval Process

- Some organizations require approval from security/admin team
- You may receive an email when approved
- Check the portal for application status

### Step 4: Retrieve Credentials

Once approved:
1. Go to your application in the W3ID portal
2. View **"Client Credentials"** section
3. Copy:
   - **Client ID**: `your-client-id-here`
   - **Client Secret**: `your-client-secret-here`

---

## Method 3: Contact IBM W3ID Support Team

If you don't have access to self-service portals:

### Option A: Email Request

Send an email to: **w3id@us.ibm.com** or **iam-support@ibm.com**

**Email Template:**
```
Subject: Request for W3ID OIDC Client Credentials

Hello W3ID Support Team,

I am requesting OAuth 2.0 client credentials for a new application:

Application Details:
- Name: Cloudant Data Extraction Control Panel
- Purpose: Internal tool for extracting and managing Cloudant database records
- Type: Web Application
- Authentication Method: OpenID Connect (OIDC)

Technical Details:
- Redirect URIs: 
  * Development: http://localhost:5000/auth/callback
  * Production: https://[your-domain].com/auth/callback
- Required Scopes: openid, profile, email
- Grant Type: Authorization Code Flow

Requestor Information:
- Name: [Your Name]
- Email: [your-email]@ibm.com
- Department: [Your Department]
- Manager: [Manager Name]

Please provide:
1. Client ID
2. Client Secret
3. OIDC Discovery URL (if different from default)

Thank you!
```

### Option B: IBM Internal Slack

1. Join the **#w3id-support** or **#iam-support** Slack channel
2. Post your request with application details
3. Support team will guide you through the process

### Option C: ServiceNow Ticket

1. Go to IBM's internal ServiceNow portal
2. Create a new ticket under **"Identity & Access Management"**
3. Select **"W3ID - OAuth Application Registration"**
4. Fill in the required details
5. Submit and wait for response

---

## Method 4: IBM API Connect (For API-Based Apps)

If your application is API-focused:

1. Navigate to: **https://api.ibm.com/**
2. Login with IBM credentials
3. Go to **"My APIs"** → **"Create API"**
4. Configure OAuth settings
5. Generate client credentials

---

## Important Information You'll Need

When registering, have this information ready:

### Application Information
- **Application Name**: Clear, descriptive name
- **Description**: Purpose and functionality
- **Application Type**: Web Application
- **Environment**: Development, Staging, Production

### Technical Configuration
- **Redirect URIs**: All callback URLs (dev + prod)
- **Logout URIs**: Post-logout redirect URLs
- **Grant Types**: Authorization Code (recommended)
- **Response Types**: code
- **Token Endpoint Auth Method**: client_secret_post or client_secret_basic

### Required Scopes
- `openid` - Required for OIDC
- `profile` - User profile information
- `email` - User email address
- Additional scopes as needed

### Contact Information
- **Technical Contact**: Your email
- **Business Owner**: Manager/sponsor email
- **Support Contact**: Team email

---

## After Receiving Credentials

### 1. Verify Credentials

Test your credentials using curl:

```bash
# Get OIDC configuration
curl https://w3id.sso.ibm.com/oidc/endpoint/default/.well-known/openid-configuration

# Test authorization endpoint (in browser)
https://w3id.sso.ibm.com/oidc/endpoint/default/authorize?
  client_id=YOUR_CLIENT_ID&
  redirect_uri=http://localhost:5000/auth/callback&
  response_type=code&
  scope=openid%20profile%20email
```

### 2. Store Credentials Securely

**Development:**
```bash
# Add to .env file (never commit to git!)
W3ID_CLIENT_ID=your_client_id_here
W3ID_CLIENT_SECRET=your_client_secret_here
```

**Production:**
- Use environment variables
- Use secret management service (AWS Secrets Manager, Azure Key Vault, etc.)
- Never hardcode in source code
- Never commit to version control

### 3. Configure Your Application

Update your `.env` file:
```bash
# W3ID OIDC Configuration
W3ID_CLIENT_ID=abc123-def456-ghi789
W3ID_CLIENT_SECRET=your-secret-key-here
W3ID_DISCOVERY_URL=https://w3id.sso.ibm.com/oidc/endpoint/default/.well-known/openid-configuration
W3ID_REDIRECT_URI=http://localhost:5000/auth/callback
W3ID_SCOPE=openid profile email
```

---

## Common W3ID Endpoints

### Production Endpoints
```
Authorization Endpoint:
https://w3id.sso.ibm.com/oidc/endpoint/default/authorize

Token Endpoint:
https://w3id.sso.ibm.com/oidc/endpoint/default/token

UserInfo Endpoint:
https://w3id.sso.ibm.com/oidc/endpoint/default/userinfo

Discovery URL:
https://w3id.sso.ibm.com/oidc/endpoint/default/.well-known/openid-configuration

JWKS URI:
https://w3id.sso.ibm.com/oidc/endpoint/default/jwk
```

### Test/Staging Endpoints
```
Discovery URL:
https://prepiam.ice.ibmcloud.com/oidc/endpoint/default/.well-known/openid-configuration
```

---

## Troubleshooting

### Issue: "Application Not Found"
**Solution**: Verify your Client ID is correct and application is approved

### Issue: "Invalid Redirect URI"
**Solution**: Ensure redirect URI in code exactly matches registered URI (including protocol and port)

### Issue: "Unauthorized Client"
**Solution**: Check that Client Secret is correct and not expired

### Issue: "Access Denied"
**Solution**: Verify required scopes are approved for your application

### Issue: "Token Expired"
**Solution**: Implement token refresh logic or re-authenticate

---

## Security Best Practices

1. **Never expose Client Secret**
   - Don't commit to git
   - Don't log in application
   - Don't send in URLs or client-side code

2. **Use HTTPS in Production**
   - Required for OAuth 2.0 security
   - Protects tokens in transit

3. **Validate Redirect URIs**
   - Only register necessary URIs
   - Use exact matches (no wildcards)

4. **Rotate Credentials Regularly**
   - Change secrets every 90 days
   - Update in all environments

5. **Monitor Usage**
   - Track authentication attempts
   - Alert on suspicious activity
   - Review access logs

---

## Additional Resources

### IBM Internal Resources
- **W3ID Documentation**: https://w3.ibm.com/w3publisher/w3id
- **IBM Cloud IAM**: https://cloud.ibm.com/docs/account?topic=account-iamoverview
- **OAuth 2.0 Guide**: https://w3.ibm.com/w3publisher/oauth

### External Resources
- **OAuth 2.0 Specification**: https://oauth.net/2/
- **OpenID Connect**: https://openid.net/connect/
- **JWT.io**: https://jwt.io/ (for debugging tokens)

### Support Channels
- **Email**: w3id@us.ibm.com
- **Slack**: #w3id-support (IBM internal)
- **ServiceNow**: Identity & Access Management category

---

## Quick Start Checklist

- [ ] Access W3ID portal or contact support
- [ ] Register application with required details
- [ ] Wait for approval (if required)
- [ ] Receive Client ID and Client Secret
- [ ] Store credentials securely in .env file
- [ ] Configure redirect URIs
- [ ] Test authentication flow
- [ ] Implement in application
- [ ] Test in development environment
- [ ] Deploy to production with production credentials

---

## Example: Complete Registration Flow

```bash
# 1. Register application (via portal or email)
Application Name: Cloudant Data Extraction
Redirect URI: http://localhost:5000/auth/callback

# 2. Receive credentials
Client ID: abc123-def456-ghi789
Client Secret: secret-key-xyz

# 3. Configure .env
echo "W3ID_CLIENT_ID=abc123-def456-ghi789" >> .env
echo "W3ID_CLIENT_SECRET=secret-key-xyz" >> .env

# 4. Test
python backend/app.py
# Navigate to http://localhost:3000
# Click "Login with W3ID"
# Complete authentication
# Verify redirect back to app
```

---

## Need Help?

If you encounter issues:

1. **Check IBM W3ID Status**: https://w3.ibm.com/status
2. **Review Documentation**: https://w3.ibm.com/w3publisher/w3id
3. **Contact Support**: w3id@us.ibm.com
4. **Ask on Slack**: #w3id-support (IBM internal)
5. **Create ServiceNow Ticket**: Identity & Access Management

---

**Last Updated**: 2026-03-24
**Maintained By**: IBM W3ID Team