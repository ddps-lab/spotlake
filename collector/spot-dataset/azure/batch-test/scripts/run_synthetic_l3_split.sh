#!/bin/bash
set -e

TIMESTAMP=$1
SAFE_TIMESTAMP=$(printf '%s' "$TIMESTAMP" | tr -c 'A-Za-z0-9_.-' '_')
COMPACTION_REQUEST_FILE="/tmp/titans_synthetic_compaction_${SAFE_TIMESTAMP}.json"

rm -f "$COMPACTION_REQUEST_FILE"

python3 collector/titans_common/queue_synthetic_l3_trigger.py \
    --provider azure \
    --env test \
    --timestamp "$TIMESTAMP" \
    --request-path "$COMPACTION_REQUEST_FILE" \
    --overwrite-month

python3 collector/titans_common/run_compaction_request.py --request "$COMPACTION_REQUEST_FILE"
rm -f "$COMPACTION_REQUEST_FILE"
