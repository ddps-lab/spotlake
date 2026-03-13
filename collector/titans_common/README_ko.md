# titans_common

[English](README.md) | [한국어](#korean)

---

<a name="korean"></a>

## 개요

**titans_common**은 SpotLake collector에서 TITANS Hot/Warm tier 통합을 위한 공용 Python 모듈입니다.

제공 기능:
- **Hot tier writer**: 멱등성이 보장되는 S3 Parquet 변경점 업로드
- **Warm tier compactor**: ETag 기반 낙관적 잠금을 사용한 m-way 점진적 컴팩션
- **멀티 프로바이더 지원**: AWS, Azure, GCP (확장 가능한 설정 구조)

## 빠른 시작

### 기본 사용법

```python
import os
from datetime import datetime, timezone

# import 전에 환경 설정
os.environ["TITANS_ENV"] = "test"  # 또는 "production"

from titans_common import prepare_for_upload, upload_hot_tier, run_compaction

# 1. DataFrame 준비 (변경 + 삭제)
combined_df = prepare_for_upload(changed_df, removed_df)

# 2. Hot tier 업로드 (10분 단위 Parquet 파일)
timestamp = datetime(2026, 1, 23, 14, 20, tzinfo=timezone.utc)
hot_key = upload_hot_tier(combined_df, timestamp, provider="aws")

# 3. Warm 컴팩션 실행 (m-way 점진적 병합)
if hot_key:
    run_compaction(hot_key, timestamp, provider="aws")
```

### 환경 설정

| 환경 | `TITANS_ENV` | S3 Prefix | 용도 |
|------|--------------|-----------|------|
| 테스트 | `test` | `test/parquet_cp_hot/...` | 프로덕션 전 검증 |
| 프로덕션 | `production` 또는 미설정 | `parquet_cp_hot/...` | 실제 운영 |

**중요**: titans_common 모듈을 import하기 전에 `TITANS_ENV`를 설정해야 합니다.

```python
# batch-test/merge_data.py
os.environ.setdefault("TITANS_ENV", "test")  # 첫 번째 줄

# batch/merge_data.py (프로덕션)
os.environ.setdefault("TITANS_ENV", "production")  # 첫 번째 줄
```

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────────┐
│                         merge_data.py                                │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐  │
│  │ Timestream  │    │ titans_common│    │ S3 (csv.gz 백업)       │  │
│  │  (기존)     │    │   (신규)     │    │                        │  │
│  └─────────────┘    └──────────────┘    └────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        titans_common                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐  │
│  │  config.py  │───▶│upload_titans │───▶│   Hot Tier (S3)        │  │
│  │  (Provider  │    │    .py       │    │ 10분 단위 Parquet      │  │
│  │   Config)   │    └──────────────┘    └────────────────────────┘  │
│  └─────────────┘            │                      │                 │
│        │                    ▼                      ▼                 │
│        │           ┌──────────────┐    ┌────────────────────────┐   │
│        └──────────▶│warm_compactor│───▶│   Warm Tier (S3)       │   │
│                    │    .py       │    │ 컴팩션된 Parquet       │   │
│                    └──────────────┘    │ + manifest.json        │   │
│                                        └────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### S3 경로 구조

```
s3://titans-spotlake-data/
├── test/                              # 테스트 환경
│   ├── parquet_cp_hot/aws/
│   │   └── 2026/01/23/14-20.parquet   # Hot tier (10분 슬롯)
│   └── parquet_warm/aws/m8/
│       └── 2026/01/
│           ├── L1_0001_00000-00007.parquet
│           ├── L2_0002_00000-00063.parquet
│           └── manifest.json
│
├── parquet_cp_hot/aws/...             # 프로덕션 Hot tier
└── parquet_warm/aws/m8/...            # 프로덕션 Warm tier
```

## API 레퍼런스

### `prepare_for_upload(changed_df, removed_df)`

변경된 DataFrame과 삭제된 DataFrame을 Ceased 컬럼 처리와 함께 병합합니다.

**매개변수:**
- `changed_df` (pd.DataFrame): 값이 변경된 레코드
- `removed_df` (pd.DataFrame): 삭제된 레코드 (Ceased=True)

**반환값:**
- `pd.DataFrame`: 업로드 준비가 완료된 병합 DataFrame

**예시:**
```python
combined = prepare_for_upload(changed_df, removed_df)
# combined: 변경된 건은 Ceased=False, 삭제된 건은 Ceased=True
```

---

### `upload_hot_tier(changed_df, timestamp, provider="aws")`

변경점을 TITANS Hot tier에 Parquet으로 업로드합니다.

**매개변수:**
- `changed_df` (pd.DataFrame): 변경점 (먼저 `prepare_for_upload` 사용)
- `timestamp` (datetime): 수집 시각 (**반드시 timezone-aware, UTC 권장**)
- `provider` (str): 클라우드 프로바이더 (`"aws"`, `"azure"`, `"gcp"`)

**반환값:**
- `str`: 업로드된 S3 key (DataFrame이 비어있으면 빈 문자열)

**예외:**
- `ValueError`: timestamp가 timezone-naive인 경우

**특징:**
- **멱등성**: `put_object` + `IfNoneMatch="*"`로 조건부 쓰기
- **결정론적 키**: 10분 슬롯 정규화 (14:23 → 14-20.parquet)
- **스키마 정규화**: `config.schema_dtypes`를 통한 dtype 캐스팅
- **압축**: 최적 저장을 위한 zstd

**예시:**
```python
from datetime import datetime, timezone

ts = datetime(2026, 1, 23, 14, 23, tzinfo=timezone.utc)
key = upload_hot_tier(df, ts, provider="aws")
# 반환: "test/parquet_cp_hot/aws/2026/01/23/14-20.parquet"
```

---

### `run_compaction(hot_s3_key, timestamp, provider="aws", timeout_seconds=30.0)`

Hot 파일 업로드 후 Warm tier 컴팩션을 실행합니다.

**매개변수:**
- `hot_s3_key` (str): `upload_hot_tier`에서 반환된 S3 키
- `timestamp` (datetime): 수집 시각 (**반드시 timezone-aware**)
- `provider` (str): 클라우드 프로바이더
- `timeout_seconds` (float): 긴 컴팩션에 대한 경고 임계값

**예외:**
- `ValueError`: timestamp가 timezone-naive인 경우
- `ConcurrencyConflictError`: 다른 프로세스가 manifest를 수정한 경우

**특징:**
- **m-way 컴팩션**: 각 레벨에서 8개 파일 → 다음 레벨로 병합
- **ETag 잠금**: manifest.json에 대한 낙관적 잠금
- **멱등성**: `processed_keys` 집합으로 중복 처리 방지
- **실패 시 롤백**: 부분 생성된 파일 정리

**예시:**
```python
try:
    run_compaction(hot_key, ts, provider="aws")
except ConcurrencyConflictError:
    print("다음 사이클에 재시도")
```

---

### `ConcurrencyConflictError`

컴팩션 중 다른 프로세스가 manifest.json을 수정했을 때 발생하는 예외입니다.

**사용법:**
```python
from titans_common import ConcurrencyConflictError

try:
    run_compaction(hot_key, ts, provider="aws")
except ConcurrencyConflictError as e:
    # 다른 프로세스가 manifest 수정 - 다음 사이클에 재시도
    print(f"충돌: {e}")
```

---

### `get_config(provider)`

프로바이더별 설정을 가져옵니다.

**매개변수:**
- `provider` (str): `"aws"`, `"azure"`, 또는 `"gcp"`

**반환값:**
- `ProviderConfig`: 설정 dataclass:
  - `pk_columns`: Primary key 컬럼들
  - `value_columns`: 값 컬럼들
  - `hot_prefix`: Hot tier S3 prefix
  - `warm_prefix`: Warm tier S3 prefix
  - `schema_dtypes`: Polars dtype 매핑
  - `canonical_columns`: 정규화를 위한 컬럼 순서

**예시:**
```python
from titans_common.config import get_config

config = get_config("aws")
print(config.pk_columns)  # ['InstanceType', 'Region', 'AZ']
print(config.hot_prefix)  # 'test/parquet_cp_hot/aws' 또는 'parquet_cp_hot/aws'
```

## 프로바이더 지원

| 프로바이더 | PK 컬럼 | 점수 컬럼 | 상태 |
|----------|---------|----------|------|
| AWS | `InstanceType, Region, AZ` | `SPS` | **활성** |
| Azure | `InstanceType, Region, AvailabilityZone` | `Score` | 계획됨 |
| GCP | `InstanceType, Region` | `Score` | 계획됨 |

### 새 프로바이더 추가

1. `config.py`에 설정 추가:
```python
PROVIDER_CONFIGS["newcloud"] = ProviderConfig(
    name="newcloud",
    pk_columns=["InstanceType", "Region"],
    value_columns=["Score", "Price"],
    score_column="Score",
)
```

2. collector에서 사용:
```python
upload_hot_tier(df, ts, provider="newcloud")
run_compaction(key, ts, provider="newcloud")
```

## 테스트

### 단위 테스트 실행

```bash
cd spotlake/collector/titans_common
uv run pytest tests/ -v
```

### 테스트 커버리지

```bash
uv run pytest tests/ -v --cov=titans_common --cov-report=term-missing
```

### 테스트 구조

```
titans_common/tests/
├── conftest.py           # 공유 fixture (moto S3 mock)
├── test_config.py        # 프로바이더 설정 테스트 (16개)
├── test_upload_titans.py # Hot tier 업로드 테스트 (11개)
└── test_warm_compactor.py# 컴팩션 테스트 (10개)
```

### 수동 E2E 테스트 (테스트 환경)

```bash
# 1. 테스트 환경 설정
export TITANS_ENV=test

# 2. batch-test에서 merge_data.py 실행
cd spotlake/collector/spot-dataset/aws/batch-test/merge
uv run python merge_data.py --sps_key "rawdata/aws/sps/2026/01/23/14-20_sps_50.pkl.gz"

# 3. Hot tier 파일 확인
aws s3 ls s3://titans-spotlake-data/test/parquet_cp_hot/aws/2026/01/23/ --profile spotrank

# 4. Warm tier manifest 확인
aws s3 cp s3://titans-spotlake-data/test/parquet_warm/aws/m8/2026/01/manifest.json - --profile spotrank | jq .
```

## 핵심 설계 결정

### 멱등성

- **Hot tier**: `put_object` + `IfNoneMatch="*"`로 파일이 한 번만 생성되도록 보장
- **Warm tier**: manifest의 `processed_keys` 집합으로 처리된 Hot 파일 추적

### 동시성 제어

- **Manifest 잠금**: ETag 기반 낙관적 잠금 (ETag 읽기 → `IfMatch`로 쓰기)
- **충돌 처리**: `ConcurrencyConflictError`가 상위로 전파되어 재시도

### 에러 격리

```python
# merge_data.py에서 - TITANS 에러가 Timestream에 영향 주지 않음
try:
    hot_key = upload_hot_tier(combined_df, ts, provider=PROVIDER)
    if hot_key:
        run_compaction(hot_key, ts, provider=PROVIDER)
except ConcurrencyConflictError as e:
    print(f"충돌, 재시도 예정: {e}")
except Exception as e:
    print(f"TITANS 실패 (치명적이지 않음): {e}")
```

### Timezone 요구사항

모든 timestamp는 **반드시 timezone-aware**여야 합니다:

```python
# 올바름
ts = datetime(2026, 1, 23, 14, 0, tzinfo=timezone.utc)

# 잘못됨 - ValueError 발생
ts = datetime(2026, 1, 23, 14, 0)  # naive datetime
```

## 의존성

```
polars>=1.37.0      # zstd를 사용한 Parquet 생성 (네이티브, pyarrow 불필요)
boto3               # S3 작업
pandas              # 입력 DataFrame 포맷
```

## 문제 해결

### "timestamp must be timezone-aware"

timestamp에 tzinfo가 있는지 확인하세요:
```python
from datetime import timezone
ts = datetime.now(timezone.utc)
# 또는
ts = naive_datetime.replace(tzinfo=timezone.utc)
```

### 업로드 시 "PreconditionFailed"

멱등성을 위한 정상적인 동작입니다 - 이전 실행에서 파일이 이미 존재합니다.

### "Manifest was modified by another process"

`ConcurrencyConflictError` - 다른 collector가 manifest를 수정했습니다. 다음 사이클에서 자동으로 재시도됩니다.

### titans_common Import 에러

collector 루트가 sys.path에 있는지 확인하세요:
```python
import sys
from pathlib import Path
COLLECTOR_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(COLLECTOR_ROOT))
```

## 관련 문서

- [TITANS 아키텍처](../../AGENTS.md)
- [Warm Tier 구현](../../agent_docs/warm_tier_implementation.md)
- [Hot Tier 분석](../../agent_docs/hot_tier_analysis.md)
- [계획 문서](../../spotlake_plan.md)
