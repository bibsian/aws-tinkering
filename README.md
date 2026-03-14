# aws_learn

Learning repo for AWS using Pluralsight cloud sandboxes.

## Prerequisites

Install these tools once:

```bash
# AWS CLI
brew install awscli

# Terraform
brew tap hashicorp/tap && brew install hashicorp/tap/terraform

# AWS SAM CLI (for serverless)
brew tap aws/tap && brew install aws-sam-cli

# Node.js (if not installed)
brew install node
```

---

## Every new sandbox session

Pluralsight sandbox credentials expire after ~4 hours. Each time you start a new sandbox:

### 1. Get your access keys

1. Log into the AWS Console with your sandbox username/password (use incognito)
2. Click your **username** (top-right corner) → **Security credentials**
3. Under **Access keys** → **Create access key**
4. Copy the **Access Key ID** and **Secret Access Key**

### 2. Configure the CLI

```bash
bash scripts/configure-sandbox.sh
```

This saves credentials to the `pluralsight` AWS profile. Nothing is committed to git.

### 3. Verify

```bash
aws sts get-caller-identity --profile pluralsight
```

---

## Terraform

```bash
cd terraform/s3-bucket

# First time only
terraform init

# Preview changes
terraform plan -var="project_name=aws-learn"

# Deploy
terraform apply -var="project_name=aws-learn"

# Tear down (important — sandbox resources may persist billing)
terraform destroy
```

---

## Serverless (Python + Lambda + API Gateway)

```bash
cd serverless/hello-api

# Build
sam build

# Deploy (first time — walks you through guided setup)
sam deploy --profile pluralsight --guided

# Deploy (subsequent times)
sam deploy --profile pluralsight

# Test locally (requires Docker)
sam local start-api
# Then: curl http://localhost:3000/hello

# Invoke function directly
sam local invoke HelloFunction
```

---

## Project structure

Each IaC framework has its own folder, with individual projects as subdirectories.
To add a new project, create a new subdirectory (e.g. `terraform/vpc/` or `serverless/my-api/`).

```
aws_learn/
├── scripts/
│   └── configure-sandbox.sh       # Set sandbox credentials each session
├── terraform/
│   └── s3-bucket/                 # Project: S3 bucket example
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
└── serverless/
    └── hello-api/                 # Project: Python Lambda + API Gateway
        ├── template.yaml
        ├── requirements.txt
        └── src/
            └── handler.py
```
