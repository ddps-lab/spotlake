#!/bin/bash
set -euo pipefail

REGION="us-west-2"
PROFILE="spotrank"
REPO_NAME="monthly-cold-freezer"
IMAGE_TAG="latest"

usage() {
    echo "Usage: $0 [-r <aws_region>] [-p <aws_profile>] [-n <repository_name>] [-t <image_tag>]"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -p: AWS Profile (default: spotrank)"
    echo "  -n: ECR repository name (default: monthly-cold-freezer)"
    echo "  -t: Image tag (default: latest)"
    exit 1
}

while getopts "r:p:n:t:" opt; do
    case "$opt" in
        r) REGION="$OPTARG" ;;
        p) PROFILE="$OPTARG" ;;
        n) REPO_NAME="$OPTARG" ;;
        t) IMAGE_TAG="$OPTARG" ;;
        *) usage ;;
    esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SPOTLAKE_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)

export AWS_PROFILE="$PROFILE"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required"
    exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
    echo "docker buildx is required"
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"

echo "Region: ${REGION}"
echo "Profile: ${AWS_PROFILE}"
echo "Repository: ${REPO_NAME}"
echo "Tag: ${IMAGE_TAG}"
echo "Image URI: ${IMAGE_URI}"
echo "Build context: ${SPOTLAKE_ROOT}"

echo "Checking ECR repository..."
aws ecr describe-repositories --repository-names "${REPO_NAME}" --region "${REGION}" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}"

echo "Logging in to ECR..."
aws ecr get-login-password --region "${REGION}" | \
    docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "Building and pushing ARM64 image..."
docker buildx build \
    --platform linux/arm64 \
    --tag "${IMAGE_URI}" \
    --file "${SPOTLAKE_ROOT}/utility/monthly_cold_freezer/dockerfile" \
    --push \
    "${SPOTLAKE_ROOT}"

echo "Successfully pushed ${IMAGE_URI}"
