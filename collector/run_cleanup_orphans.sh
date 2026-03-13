#!/bin/bash
# TITANS Orphan Cleanup Script
# orphan = manifest에 없는 S3 잔여 파일 (IAM 권한 누락 등으로 삭제 실패 시 누적)

# --- Test 환경 ---

# Dry run (기본값, 삭제하지 않고 목록만 출력)
TITANS_ENV=test uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --profile spotrank

# 실제 삭제
# TITANS_ENV=test uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --profile spotrank --execute

# --- Production 환경 ---

# TITANS_ENV=production uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --profile spotrank
# TITANS_ENV=production uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --profile spotrank --execute

# --- 특정 provider ---

# TITANS_ENV=test uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --provider azure --profile spotrank
# TITANS_ENV=test uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --provider gcp --profile spotrank
