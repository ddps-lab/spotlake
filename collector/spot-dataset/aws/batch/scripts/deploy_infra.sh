#!/bin/bash
set -e

# Default values
AWS_REGION="us-west-2"
S3_BUCKET="spotlake"

usage() {
    echo "Usage: $0 -v <vpc_id> -s <subnet_ids> -g <security_group_ids> -i <image_uri> [-r <aws_region>] [-b <s3_bucket>] [-p <aws_profile>] [-w <slack_webhook_url>]"
    echo "  -v: VPC ID"
    echo "  -s: Subnet IDs (JSON format, e.g., '[\"subnet-1\", \"subnet-2\"]')"
    echo "  -g: Security Group IDs (JSON format, e.g., '[\"sg-1\"]')"
    echo "  -i: Docker Image URI"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -b: S3 Bucket Name (default: spotlake)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    echo "  -w: Slack Webhook URL (optional, enables failure monitoring if provided)"
    exit 1
}

while getopts "v:s:g:i:r:b:p:w:" opt; do
    case $opt in
        v) VPC_ID="$OPTARG" ;;
        s) SUBNET_IDS="$OPTARG" ;;
        g) SECURITY_GROUP_IDS="$OPTARG" ;;
        i) IMAGE_URI="$OPTARG" ;;
        r) AWS_REGION="$OPTARG" ;;
        b) S3_BUCKET="$OPTARG" ;;
        p) AWS_PROFILE="$OPTARG" ;;
        w) SLACK_WEBHOOK_URL="$OPTARG" ;;
        *) usage ;;
    esac
done

MISSING_ARGS=false

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

# Set AWS profile for all subsequent AWS CLI and Terraform commands
if [ -n "$AWS_PROFILE" ]; then
    export AWS_PROFILE="$AWS_PROFILE"
    echo "Using AWS Profile: $AWS_PROFILE"
fi

echo "Deploying Infrastructure..."
cd collector/spot-dataset/aws/batch/infrastructure

echo "Initializing Terraform..."
terraform init

echo "Applying Terraform..."

# Build terraform variables
TF_VARS=(
    -var "vpc_id=$VPC_ID"
    -var "subnet_ids=$SUBNET_IDS"
    -var "security_group_ids=$SECURITY_GROUP_IDS"
    -var "image_uri=$IMAGE_URI"
    -var "aws_region=$AWS_REGION"
    -var "s3_bucket=$S3_BUCKET"
)

# Add Slack webhook if provided
if [ -n "$SLACK_WEBHOOK_URL" ]; then
    echo "Batch failure monitoring will be enabled"
    
    # Check if Lambda already exists (created by another batch deployment)
    if aws lambda get-function --function-name batch-failure-notifier &>/dev/null; then
        echo "Found existing batch-failure-notifier Lambda - will reuse"
        TF_VARS+=(-var "use_existing_lambda=true")
    else
        echo "No existing Lambda found - will create new one"
        TF_VARS+=(-var "use_existing_lambda=false")
    fi
    
    TF_VARS+=(-var "slack_webhook_url=$SLACK_WEBHOOK_URL")
else
    echo "Batch failure monitoring disabled (no webhook URL provided)"
fi

terraform apply -auto-approve "${TF_VARS[@]}"

echo "Deployment Complete."
