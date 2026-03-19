#!/bin/bash
set -e

HOT_KEY=$1
TIMESTAMP=$2

python3 collector/titans_common/run_compaction_once.py \
    --provider azure \
    --hot-key "$HOT_KEY" \
    --timestamp "$TIMESTAMP" \
    --timeout-seconds 120.0
