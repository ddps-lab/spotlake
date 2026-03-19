#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAMBDA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

AWS_REGION="${AWS_REGION:-us-west-2}"
LAYER_NAME="${LAYER_NAME:-gcp-collector-titans-deps}"
RUNTIME="${RUNTIME:-python3.12}"
ARCHITECTURE="${ARCHITECTURE:-arm64}"
PACKAGES="${PACKAGES:-polars==1.37.0}"
OUTPUT_ZIP="${OUTPUT_ZIP:-${LAMBDA_DIR}/dist/${LAYER_NAME}-${RUNTIME}-${ARCHITECTURE}.zip}"
PUBLISH_LAYER="${PUBLISH_LAYER:-0}"
LAYER_BUCKET="${LAYER_BUCKET:-titans-spotlake-data}"
LAYER_DESCRIPTION="${LAYER_DESCRIPTION:-TITANS deps layer (${RUNTIME}, ${ARCHITECTURE})}"

runtime_tag="${RUNTIME#python}"
if [ "${runtime_tag}" = "${RUNTIME}" ]; then
  echo "RUNTIME must look like python3.12 or python3.9"
  exit 1
fi

case "${ARCHITECTURE}" in
  x86_64)
    docker_platform="linux/amd64"
    ;;
  arm64)
    docker_platform="linux/arm64"
    ;;
  *)
    echo "ARCHITECTURE must be x86_64 or arm64"
    exit 1
    ;;
esac

mkdir -p "${LAMBDA_DIR}/.build"
build_dir="$(mktemp -d "${LAMBDA_DIR}/.build/layer-${RUNTIME}-${ARCHITECTURE}-XXXXXX")"
mkdir -p "${build_dir}/python" "${LAMBDA_DIR}/dist"

docker run --rm \
  --platform "${docker_platform}" \
  --entrypoint /bin/bash \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e PIP_PACKAGES="${PACKAGES}" \
  -v "${build_dir}:/asset" \
  "public.ecr.aws/lambda/python:${runtime_tag}" \
  -lc "python -m pip install --no-cache-dir -t /asset/python \$PIP_PACKAGES"

rm -f "${OUTPUT_ZIP}"
(cd "${build_dir}" && zip -qr "${OUTPUT_ZIP}" python)
echo "Built layer zip: ${OUTPUT_ZIP}"

if [ "${PUBLISH_LAYER}" = "1" ]; then
  key="tmp/layers/${LAYER_NAME}/$(date +%Y%m%d-%H%M%S)-${RUNTIME}-${ARCHITECTURE}.zip"
  aws s3 cp "${OUTPUT_ZIP}" "s3://${LAYER_BUCKET}/${key}" --region "${AWS_REGION}"

  layer_arn="$(aws lambda publish-layer-version \
    --layer-name "${LAYER_NAME}" \
    --content "S3Bucket=${LAYER_BUCKET},S3Key=${key}" \
    --compatible-runtimes "${RUNTIME}" \
    --compatible-architectures "${ARCHITECTURE}" \
    --description "${LAYER_DESCRIPTION}" \
    --region "${AWS_REGION}" \
    --query "LayerVersionArn" \
    --output text)"

  echo "Published layer: ${layer_arn}"
  echo "Add this ARN into iac tfvars -> extra_layer_arns"
fi
