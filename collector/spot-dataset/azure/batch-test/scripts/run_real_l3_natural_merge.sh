#!/bin/bash
set -e

TIMESTAMP=$1
SAFE_TIMESTAMP=$(printf '%s' "$TIMESTAMP" | tr -c 'A-Za-z0-9_.-' '_')
COMPACTION_REQUEST_FILE="/tmp/titans_compaction_${SAFE_TIMESTAMP}.json"

rm -f "$COMPACTION_REQUEST_FILE"

TITANS_COMPACTION_REQUEST_PATH="$COMPACTION_REQUEST_FILE" \
    python3 collector/spot-dataset/azure/batch-test/merge/merge_data.py --timestamp "$TIMESTAMP"

if [ -f "$COMPACTION_REQUEST_FILE" ]; then
    python3 collector/titans_common/run_compaction_request.py --request "$COMPACTION_REQUEST_FILE"
    rm -f "$COMPACTION_REQUEST_FILE"
fi
