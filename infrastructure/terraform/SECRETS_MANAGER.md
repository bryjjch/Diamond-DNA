# AWS Secrets Manager Integration

This infrastructure now supports using AWS Secrets Manager to store sensitive credentials instead of hardcoding them in Terraform variables or environment variables.

## Supported Secrets

1. **MLB API Key** - Used by the scraper Lambda function
2. **OpenSearch Credentials** - Used by OpenSearch domain and Lambda functions (username and password)

## Secret Formats

### MLB API Key Secret
The secret should be stored as JSON with the following structure:
```json
{
  "api_key": "your-mlb-api-key-here"
}
```

### OpenSearch Credentials Secret
The secret should be stored as JSON with the following structure:
```json
{
  "username": "admin",
  "password": "your-opensearch-password-here"
}
```

## Usage

**Secrets must be created in AWS Secrets Manager before deploying infrastructure.** The infrastructure requires secret ARNs and does not support automatic secret creation or raw credential values.

### Creating Secrets

Create secrets manually in AWS Secrets Manager, then reference them by ARN:

```hcl
# terraform.tfvars
# These are REQUIRED - secrets must exist in AWS Secrets Manager
mlb_api_key_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:mlb-api-key-abc123"
opensearch_credentials_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:opensearch-creds-xyz789"
```

## Lambda Function Updates Required

Your Lambda functions need to be updated to retrieve secrets from AWS Secrets Manager. Here's an example for Python:

### Example: Retrieving MLB API Key

```python
import boto3
import json
import os

def get_secret(secret_arn):
    """Retrieve secret from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    return json.loads(response['SecretString'])

def get_mlb_api_key():
    """Get MLB API key from Secrets Manager"""
    secret_arn = os.environ.get('MLB_API_KEY_SECRET_ARN')
    if not secret_arn:
        raise ValueError("MLB_API_KEY_SECRET_ARN environment variable is required")
    
    secret = get_secret(secret_arn)
    return secret.get('api_key')
```

### Example: Retrieving OpenSearch Credentials

```python
def get_opensearch_credentials():
    """Get OpenSearch credentials from Secrets Manager"""
    secret_arn = os.environ.get('OPENSEARCH_CREDENTIALS_SECRET_ARN')
    if not secret_arn:
        raise ValueError("OPENSEARCH_CREDENTIALS_SECRET_ARN environment variable is required")
    
    secret = get_secret(secret_arn)
    return {
        'username': secret.get('username'),
        'password': secret.get('password')
    }
```

## IAM Permissions

The Lambda execution roles have been automatically granted permissions to read from Secrets Manager. The following permissions are included:

- `secretsmanager:GetSecretValue`
- `secretsmanager:DescribeSecret`

These permissions are scoped to only the specific secrets used by each Lambda function.

## Setup Guide

### Initial Setup

1. **Create secrets in AWS Secrets Manager** (via AWS Console or CLI):
   ```bash
   # Create MLB API key secret
   aws secretsmanager create-secret \
     --name "DiamondDNA-mlb-api-key-dev" \
     --secret-string '{"api_key":"your-api-key"}'
   
   # Create OpenSearch credentials secret
   aws secretsmanager create-secret \
     --name "DiamondDNA-opensearch-credentials-dev" \
     --secret-string '{"username":"admin","password":"your-password"}'
   ```

2. **Get the secret ARNs** from the AWS Console or CLI:
   ```bash
   aws secretsmanager describe-secret --secret-id "DiamondDNA-mlb-api-key-dev" --query 'ARN' --output text
   aws secretsmanager describe-secret --secret-id "DiamondDNA-opensearch-credentials-dev" --query 'ARN' --output text
   ```

3. **Update your Terraform variables** (`terraform.tfvars`) to use secret ARNs:
   ```hcl
   mlb_api_key_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:DiamondDNA-mlb-api-key-dev-abc123"
   opensearch_credentials_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:DiamondDNA-opensearch-credentials-dev-xyz789"
   ```

4. **Update Lambda function code** to retrieve secrets at runtime (see examples above)

5. **Apply Terraform changes**:
   ```bash
   terraform plan
   terraform apply
   ```

## Security Best Practices

1. **Never commit secrets to version control** - Always use Secrets Manager or environment variables
2. **Use least privilege** - IAM roles only have access to the specific secrets they need
3. **Enable secret rotation** - Consider enabling automatic rotation for passwords
4. **Use separate secrets per environment** - Different secrets for dev, staging, and production
5. **Monitor secret access** - Enable CloudTrail to audit secret access

## Required Variables

The following variables are **required** and must be provided:
- `mlb_api_key_secret_arn` - ARN of the MLB API key secret
- `opensearch_credentials_secret_arn` - ARN of the OpenSearch credentials secret

These secrets must exist in AWS Secrets Manager before deploying the infrastructure. The infrastructure does not support automatic secret creation or raw credential values.
