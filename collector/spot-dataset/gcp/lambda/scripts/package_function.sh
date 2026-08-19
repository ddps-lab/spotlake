#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAMBDA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../../.." && pwd)"

AWS_REGION="${AWS_REGION:-us-west-2}"
FUNCTION_NAME="${FUNCTION_NAME:-gcp-collector}"
OUT_ZIP="${OUT_ZIP:-${LAMBDA_DIR}/dist/gcp-collector.zip}"
WORK_DIR="${LAMBDA_DIR}/.build/package"

CRED_FILE="${CRED_FILE:-}"
SKIP_FETCH_CURRENT="${SKIP_FETCH_CURRENT:-0}"

mkdir -p "${LAMBDA_DIR}/dist"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/utility" "${WORK_DIR}/titans_common"

copy_required_files() {
  cp "${LAMBDA_DIR}/lambda_function.py" "${WORK_DIR}/"
  cp "${LAMBDA_DIR}/collector_core.py" "${WORK_DIR}/"
  cp "${LAMBDA_DIR}/compare_data.py" "${WORK_DIR}/"
  cp "${LAMBDA_DIR}/runtime_config.py" "${WORK_DIR}/"
  cp "${LAMBDA_DIR}/s3_management.py" "${WORK_DIR}/"
  cp "${REPO_ROOT}/spotlake/const_config.py" "${WORK_DIR}/"
  cp "${REPO_ROOT}/spotlake/utility/slack_msg_sender.py" "${WORK_DIR}/utility/"

  cp "${REPO_ROOT}/spotlake/collector/titans_common/__init__.py" "${WORK_DIR}/titans_common/"
  cp "${REPO_ROOT}/spotlake/collector/titans_common/config.py" "${WORK_DIR}/titans_common/"
  cp "${REPO_ROOT}/spotlake/collector/titans_common/partitioned_eager_merge.py" "${WORK_DIR}/titans_common/"
  cp "${REPO_ROOT}/spotlake/collector/titans_common/upload_titans.py" "${WORK_DIR}/titans_common/"
  cp "${REPO_ROOT}/spotlake/collector/titans_common/warm_compactor.py" "${WORK_DIR}/titans_common/"
  cp "${REPO_ROOT}/spotlake/collector/titans_common/utils.py" "${WORK_DIR}/titans_common/"
}

fetch_credentials_from_live() {
  local cfg_json cred_name code_url temp_zip
  cfg_json="$(aws lambda get-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --region "${AWS_REGION}" \
    --output json)"

  cred_name="$(echo "${cfg_json}" | jq -r '.Environment.Variables.GOOGLE_APPLICATION_CREDENTIALS // empty')"
  if [ -z "${cred_name}" ]; then
    echo "No GOOGLE_APPLICATION_CREDENTIALS in live function env; skipping credential fetch."
    return
  fi

  code_url="$(aws lambda get-function \
    --function-name "${FUNCTION_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Code.Location' \
    --output text)"

  temp_zip="${WORK_DIR}/_live_code.zip"
  curl -sSL "${code_url}" -o "${temp_zip}"

  if unzip -l "${temp_zip}" "${cred_name}" >/dev/null 2>&1; then
    unzip -p "${temp_zip}" "${cred_name}" > "${WORK_DIR}/${cred_name}"
    echo "Fetched credentials file from current Lambda code: ${cred_name}"
  else
    echo "Credential file ${cred_name} not found in current Lambda zip."
  fi
}

copy_credentials() {
  if [ -n "${CRED_FILE}" ]; then
    cp "${CRED_FILE}" "${WORK_DIR}/"
    echo "Copied credentials from local file: ${CRED_FILE}"
    return
  fi

  if [ "${SKIP_FETCH_CURRENT}" = "1" ]; then
    echo "SKIP_FETCH_CURRENT=1 set; not fetching credentials from live function."
    return
  fi

  fetch_credentials_from_live
}

copy_required_files
copy_credentials

rm -f "${OUT_ZIP}"
(cd "${WORK_DIR}" && zip -qr "${OUT_ZIP}" .)

echo "Built function package: ${OUT_ZIP}"
