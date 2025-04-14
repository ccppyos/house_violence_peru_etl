# Terraform Infrastructure for Violence Cases Pipeline

This directory contains Terraform configurations for provisioning AWS infrastructure required for the violence cases data pipeline.

## Project Structure

terraform/
├── main.tf # Main infrastructure configuration
├── providers.tf # AWS provider configuration
├── variables.tf # Variable definitions
├── .env # Environment variables (not tracked in git)
└── terraform_pf.ps1 # PowerShell script for env setup
```

## Prerequisites
- [Terraform](https://www.terraform.io/downloads.html) installed
- AWS credentials configured
- PowerShell (for Windows users)

## Environment Setup

1. Create `.env` file with your AWS credentials:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

2. Run the PowerShell script to set environment variables:
```powershell
.\terraform_pf.ps1
```

## Infrastructure Components

The Terraform configuration creates:
- S3 bucket for data storage
- Redshift cluster setup

## Usage

1. Initialize Terraform:
```bash
terraform init
```

2. Review planned changes:
```bash
terraform plan
```

3. Apply the configuration:
```bash
terraform apply
```

4. To destroy the infrastructure:
```bash
terraform destroy
```

## Important Files

### main.tf
Contains the main infrastructure definitions including:
- Resource configurations
- Service setups
- Security configurations

### variables.tf
Defines variables used across the configuration:
- AWS region
- Resource naming
- Service configurations

### providers.tf
Configures the AWS provider and required versions.
