#!/bin/bash
set -e

# Default values
AWS_REGION="us-west-2"
S3_BUCKET="spotlake"
TITANS_BUCKET="titans-spotlake-data"

usage() {
    echo "Usage: $0 -v <vpc_id> -s <subnet_ids> -g <security_group_ids> -i <image_uri> [-r <aws_region>] [-b <s3_bucket>] [-t <titans_bucket>] [-p <aws_profile>] [-w <slack_webhook_url>]"
    echo "  -v: VPC ID"
    echo "  -s: Subnet IDs (JSON format, e.g., '[\"subnet-1\", \"subnet-2\"]')"
    echo "  -g: Security Group IDs (JSON format, e.g., '[\"sg-1\"]')"
    echo "  -i: Docker Image URI"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -b: S3 Bucket Name (default: spotlake)"
    echo "  -t: TITANS Bucket Name (default: titans-spotlake-data)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    echo "  -w: Slack Webhook URL (default: SSM parameter error_notification_slack_webhook_url)"
    exit 1
}

while getopts "v:s:g:i:r:b:t:p:w:" opt; do
    case $opt in
        v) VPC_ID="$OPTARG" ;;
        s) SUBNET_IDS="$OPTARG" ;;
        g) SECURITY_GROUP_IDS="$OPTARG" ;;
        i) IMAGE_URI="$OPTARG" ;;
        r) AWS_REGION="$OPTARG" ;;
        b) S3_BUCKET="$OPTARG" ;;
        t) TITANS_BUCKET="$OPTARG" ;;
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

echo "Initializing Terraform (remote state in s3://spotlake-terraform-state)..."
terraform init -input=false

echo "Applying Terraform..."

# Build terraform variables
TF_VARS=(
    -var "vpc_id=$VPC_ID"
    -var "subnet_ids=$SUBNET_IDS"
    -var "security_group_ids=$SECURITY_GROUP_IDS"
    -var "image_uri=$IMAGE_URI"
    -var "aws_region=$AWS_REGION"
    -var "s3_bucket=$S3_BUCKET"
    -var "titans_bucket=$TITANS_BUCKET"
)

# Failure monitoring is part of the managed production stack. Ownership of the
# shared batch-failure-notifier Lambda is fixed in production.auto.tfvars.json
# (the Azure stack creates it and the AWS stack reads it), so it is not auto-detected.
# Without -w, use the SSM parameter the collectors already read for Slack alerts.
if [ -z "$SLACK_WEBHOOK_URL" ]; then
    SLACK_WEBHOOK_URL=$(aws ssm get-parameter --name error_notification_slack_webhook_url \
        --region "$AWS_REGION" --query Parameter.Value --output text 2>/dev/null || true)
fi
if [ -z "$SLACK_WEBHOOK_URL" ] || [ "$SLACK_WEBHOOK_URL" = "None" ]; then
    echo "Error: Slack Webhook URL not found (-w or SSM error_notification_slack_webhook_url)."
    echo "       Without it Terraform would remove failure monitoring."
    exit 1
fi
# Pass the webhook through the environment so it does not appear in process arguments.
export TF_VAR_slack_webhook_url="$SLACK_WEBHOOK_URL"

# Review the plan before applying. The plan file holds variable values, so keep it outside the repo.
PLAN_FILE=$(mktemp -t spotlake-tfplan.XXXXXX)
trap 'rm -f "$PLAN_FILE"' EXIT
terraform plan -input=false -out="$PLAN_FILE" "${TF_VARS[@]}"

read -r -p "Apply this plan? Type 'yes' to continue: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted. No changes were applied."
    exit 1
fi

terraform apply -input=false "$PLAN_FILE"

echo "Deployment Complete."
