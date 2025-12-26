#!/bin/bash
set -e

# Default values
REGION="us-west-2"
REPO_NAME="spotlake-azure-batch-test"

usage() {
    echo "Usage: $0 [-r <aws_region>] [-p <aws_profile>] [-a <access_key_id>] [-s <secret_access_key>]"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    echo "  -a: AWS Access Key ID"
    echo "  -s: AWS Secret Access Key"
    exit 1
}

while getopts "r:p:a:s:" opt; do
    case $opt in
        r) REGION="$OPTARG" ;;
        p) export AWS_PROFILE="$OPTARG" ;;
        a) ACCESS_KEY="$OPTARG" ;;
        s) SECRET_KEY="$OPTARG" ;;
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
    echo "Using AWS Profile: $AWS_PROFILE"
fi

# Check if credentials are provided
if [ -z "$ACCESS_KEY" ] || [ -z "$SECRET_KEY" ]; then
    echo "Warning: AWS Credentials not provided. Image will be built without default credentials."
fi

# Create ECR repository if it doesn't exist
echo "Checking ECR repository..."
aws ecr describe-repositories --repository-names "${REPO_NAME}" --region "${REGION}" > /dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}"

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

# Build Docker image
echo "Building Docker image..."
# Note: Path to Dockerfile is relative to build context (project root)
# The context is the project root, so we point to the Dockerfile in azure/batch-test
docker build \
    --platform linux/amd64 \
    --build-arg AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
    --build-arg AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    -t "$REPO_NAME" \
    -f collector/spot-dataset/azure/batch-test/Dockerfile .

# Tag Docker image
echo "Tagging Docker image..."
docker tag "${REPO_NAME}:latest" "${IMAGE_URI}"

# Push Docker image
echo "Pushing Docker image..."
docker push "${IMAGE_URI}"

echo "Successfully built and pushed ${IMAGE_URI}"
