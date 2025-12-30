#!/bin/bash
set -e

# Default values
AWS_REGION="us-west-2"
S3_BUCKET="spotlake-test"

usage() {
    echo "Usage: $0 -v <vpc_id> -s <subnet_ids> -g <security_group_ids> -i <image_uri> [-r <aws_region>] [-b <s3_bucket>] [-p <aws_profile>]"
    echo "  -v: VPC ID"
    echo "  -s: Subnet IDs (JSON format, e.g., '[\"subnet-1\", \"subnet-2\"]')"
    echo "  -g: Security Group IDs (JSON format, e.g., '[\"sg-1\"]')"
    echo "  -i: Docker Image URI"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -b: S3 Bucket Name (default: spotlake-test)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    exit 1
}

while getopts "v:s:g:i:r:b:p:" opt; do
    case $opt in
        v) VPC_ID="$OPTARG" ;;
        s) SUBNET_IDS="$OPTARG" ;;
        g) SECURITY_GROUP_IDS="$OPTARG" ;;
        i) IMAGE_URI="$OPTARG" ;;
        r) AWS_REGION="$OPTARG" ;;
        b) S3_BUCKET="$OPTARG" ;;
        p) export AWS_PROFILE="$OPTARG" ;;
        *) usage ;;
    esac
done

MISSING_ARGS=false

# Note: Destroy might not strictly require all variables if state is present, 
# but providing them is safer for consistency with variables.tf definition.
if [ -z "$VPC_ID" ]; then
    echo "Error: VPC ID (-v) is required."
    MISSING_ARGS=true
fi

if [ -z "$SUBNET_IDS" ]; then
    echo "Error: Subnet IDs (-s) are required."
    MISSING_ARGS=true
fi

if [ -z "$SECURITY_GROUP_IDS" ]; then
    echo "Error: Security Group IDs (-g) are required."
    MISSING_ARGS=true
fi

if [ -z "$IMAGE_URI" ]; then
    echo "Error: Docker Image URI (-i) is required."
    MISSING_ARGS=true
fi

if [ "$MISSING_ARGS" = true ]; then
    usage
fi

echo "Destroying SpotLake Azure Test Infrastructure..."
cd collector/spot-dataset/azure/batch-test/infrastructure

echo "Initializing Terraform..."
terraform init

echo "Destroying Terraform..."
terraform destroy -auto-approve \
    -var "vpc_id=$VPC_ID" \
    -var "subnet_ids=$SUBNET_IDS" \
    -var "security_group_ids=$SECURITY_GROUP_IDS" \
    -var "image_uri=$IMAGE_URI" \
    -var "aws_region=$AWS_REGION" \
    -var "s3_bucket=$S3_BUCKET"

echo "Destruction Complete."
