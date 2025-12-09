#!/bin/bash
set -e

# Default values
REGION="us-west-2"
REPO_NAME="spotlake-batch-test"

usage() {
    echo "Usage: $0 [-r <aws_region>] [-p <aws_profile>] [-t <image_tag>]"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    echo "  -t: Image tag to delete (default: latest)"
    exit 1
}

IMAGE_TAG="latest"

while getopts "r:p:t:" opt; do
    case $opt in
        r) REGION="$OPTARG" ;;
        p) export AWS_PROFILE="$OPTARG" ;;
        t) IMAGE_TAG="$OPTARG" ;;
        *) usage ;;
    esac
done

echo "Region: $REGION"
echo "Repository: $REPO_NAME"
echo "Image Tag: $IMAGE_TAG"
if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
fi

# Delete image from ECR
echo "Deleting image from ECR..."
aws ecr batch-delete-image \
    --repository-name "$REPO_NAME" \
    --image-ids imageTag="$IMAGE_TAG" \
    --region "$REGION"

echo "Successfully deleted image $REPO_NAME:$IMAGE_TAG"
