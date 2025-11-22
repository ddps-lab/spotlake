#!/bin/bash
set -e

# Check if required environment variables are set
if [ -z "$TF_VAR_vpc_id" ]; then
    echo "Error: TF_VAR_vpc_id is not set."
    exit 1
fi

if [ -z "$TF_VAR_subnet_ids" ]; then
    echo "Error: TF_VAR_subnet_ids is not set. Format: '[\"subnet-1\", \"subnet-2\"]'"
    exit 1
fi

if [ -z "$TF_VAR_security_group_ids" ]; then
    echo "Error: TF_VAR_security_group_ids is not set. Format: '[\"sg-1\"]'"
    exit 1
fi

if [ -z "$TF_VAR_image_uri" ]; then
    echo "Error: TF_VAR_image_uri is not set."
    exit 1
fi

echo "Deploying Infrastructure..."
cd collector/spot-dataset/aws/batch/infrastructure

echo "Initializing Terraform..."
terraform init

echo "Applying Terraform..."
terraform apply -auto-approve

echo "Deployment Complete."
