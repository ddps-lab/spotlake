#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_JSON="$(cat)"

mode="$(echo "${QUERY_JSON}" | jq -r '.mode')"
output_zip="$(echo "${QUERY_JSON}" | jq -r '.output_zip')"
cache_key="$(echo "${QUERY_JSON}" | jq -r '.cache_key')"

if [ -z "${mode}" ] || [ -z "${output_zip}" ] || [ -z "${cache_key}" ]; then
  echo "mode, output_zip, and cache_key are required" >&2
  exit 1
fi

mkdir -p "$(dirname "${output_zip}")"
cache_file="${output_zip}.terraform-cache-key"

build_artifact() {
  case "${mode}" in
    package)
      local credential_file function_name aws_region
      credential_file="$(echo "${QUERY_JSON}" | jq -r '.credential_file')"
      function_name="$(echo "${QUERY_JSON}" | jq -r '.function_name')"
      aws_region="$(echo "${QUERY_JSON}" | jq -r '.aws_region')"
      if [ -z "${credential_file}" ] || [ "${credential_file}" = "null" ]; then
        echo "credential_file is required for package mode" >&2
        exit 1
      fi
      CRED_FILE="${credential_file}" \
      FUNCTION_NAME="${function_name}" \
      AWS_REGION="${aws_region}" \
      OUT_ZIP="${output_zip}" \
      "${SCRIPT_DIR}/package_function.sh" >&2
      ;;
    base_layer)
      local aws_region layer_name runtime architecture packages
      aws_region="$(echo "${QUERY_JSON}" | jq -r '.aws_region')"
      layer_name="$(echo "${QUERY_JSON}" | jq -r '.layer_name')"
      runtime="$(echo "${QUERY_JSON}" | jq -r '.runtime')"
      architecture="$(echo "${QUERY_JSON}" | jq -r '.architecture')"
      packages="$(echo "${QUERY_JSON}" | jq -r '.packages')"
      AWS_REGION="${aws_region}" \
      LAYER_NAME="${layer_name}" \
      RUNTIME="${runtime}" \
      ARCHITECTURE="${architecture}" \
      PACKAGES="${packages}" \
      OUTPUT_ZIP="${output_zip}" \
      PUBLISH_LAYER=0 \
      "${SCRIPT_DIR}/build_base_layer.sh" >&2
      ;;
    titans_layer)
      local aws_region layer_name runtime architecture packages
      aws_region="$(echo "${QUERY_JSON}" | jq -r '.aws_region')"
      layer_name="$(echo "${QUERY_JSON}" | jq -r '.layer_name')"
      runtime="$(echo "${QUERY_JSON}" | jq -r '.runtime')"
      architecture="$(echo "${QUERY_JSON}" | jq -r '.architecture')"
      packages="$(echo "${QUERY_JSON}" | jq -r '.packages')"
      AWS_REGION="${aws_region}" \
      LAYER_NAME="${layer_name}" \
      RUNTIME="${runtime}" \
      ARCHITECTURE="${architecture}" \
      PACKAGES="${packages}" \
      OUTPUT_ZIP="${output_zip}" \
      PUBLISH_LAYER=0 \
      "${SCRIPT_DIR}/build_layer.sh" >&2
      ;;
    *)
      echo "unsupported mode: ${mode}" >&2
      exit 1
      ;;
  esac
}

if [ ! -f "${output_zip}" ] || [ ! -f "${cache_file}" ] || [ "$(cat "${cache_file}")" != "${cache_key}" ]; then
  build_artifact
  printf '%s' "${cache_key}" > "${cache_file}"
fi

source_code_hash="$(openssl dgst -binary -sha256 "${output_zip}" | openssl base64 -A)"

jq -n \
  --arg filename "${output_zip}" \
  --arg source_code_hash "${source_code_hash}" \
  '{filename: $filename, source_code_hash: $source_code_hash}'
