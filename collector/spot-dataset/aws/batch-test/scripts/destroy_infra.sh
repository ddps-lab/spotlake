#!/bin/bash
set -e

# Default values
REGION="us-west-2"

usage() {
    echo "Usage: $0 -v <vpc_id> -s <subnet_ids> -g <security_group_ids> -i <image_uri> [-r <aws_region>] [-p <aws_profile>]"
    echo "  -v: VPC ID (required)"
    echo "  -s: Subnet IDs as JSON array string, e.g. '[\"subnet-xxx\",\"subnet-yyy\"]' (required)"
    echo "  -g: Security Group IDs as JSON array string, e.g. '[\"sg-xxx\"]' (required)"
    echo "  -i: Docker Image URI (required)"
    echo "  -r: AWS Region (default: us-west-2)"
    echo "  -p: AWS Profile (optional, uses default if not set)"
    exit 1
}

while getopts "v:s:g:i:r:p:" opt; do
    case $opt in
        v) VPC_ID="$OPTARG" ;;
        s) SUBNET_IDS="$OPTARG" ;;
        g) SECURITY_GROUP_IDS="$OPTARG" ;;
        i) IMAGE_URI="$OPTARG" ;;
        r) REGION="$OPTARG" ;;
        p) export AWS_PROFILE="$OPTARG" ;;
        *) usage ;;
    esac
done

# Check required parameters
if [ -z "$VPC_ID" ] || [ -z "$SUBNET_IDS" ] || [ -z "$SECURITY_GROUP_IDS" ] || [ -z "$IMAGE_URI" ]; then
    echo "Error: Missing required parameters"
    usage
fi

echo "Region: $REGION"
echo "VPC ID: $VPC_ID"
echo "Subnet IDs: $SUBNET_IDS"
echo "Security Group IDs: $SECURITY_GROUP_IDS"
echo "Image URI: $IMAGE_URI"
if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
fi

# Navigate to infrastructure directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infrastructure"

cd "$INFRA_DIR"

echo "Destroying Infrastructure..."
terraform destroy -auto-approve \
    -var "aws_region=$REGION" \
    -var "vpc_id=$VPC_ID" \
    -var "subnet_ids=$SUBNET_IDS" \
    -var "security_group_ids=$SECURITY_GROUP_IDS" \
    -var "image_uri=$IMAGE_URI"

echo "Infrastructure destroyed successfully"
