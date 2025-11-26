#!/bin/bash
set -e

# Default values
REGION="us-west-2"
REPO_NAME="spotlake-batch-test"

usage() {
    echo "Usage: $0 [-r <aws_region>] [-p <aws_profile>]"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    exit 1
}

while getopts "r:p:" opt; do
    case $opt in
        r) REGION="$OPTARG" ;;
        p) export AWS_PROFILE="$OPTARG" ;;
        *) usage ;;
    esac
done

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:latest"

echo "Region: $REGION"
echo "Repository: $REPO_NAME"
echo "Account ID: $ACCOUNT_ID"
echo "Image URI: $IMAGE_URI"
if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
fi

# Create ECR repository if it doesn't exist
echo "Checking ECR repository..."
aws ecr describe-repositories --repository-names "${REPO_NAME}" --region "${REGION}" > /dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}"

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

# Build Docker image
echo "Building Docker image..."
# Assuming script is run from project root
docker build -t "${REPO_NAME}" -f collector/spot-dataset/aws/batch-test/Dockerfile collector/spot-dataset/aws/batch-test/

# Tag Docker image
echo "Tagging Docker image..."
docker tag "${REPO_NAME}:latest" "${IMAGE_URI}"

# Push Docker image
echo "Pushing Docker image..."
docker push "${IMAGE_URI}"

echo "Successfully built and pushed ${IMAGE_URI}"
