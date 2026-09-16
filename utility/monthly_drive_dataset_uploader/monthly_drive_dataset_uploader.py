"""SpotLake 공개 데이터셋을 월별 zip으로 만들어 Google Drive에 올린다.

원본 Rawdata CSV를 날짜별 parquet으로 펼친 뒤 한 달치를 zip 하나로 묶어
0.5년~1.5년 전 구간만 Drive에 유지한다.

    s3://spotlake/rawdata/{provider}/{YYYY}/{MM}/{DD}/{HH-MM-SS}.csv.gz
        -> Drive: {provider}/{provider}-{YYYY-MM}.zip
                  안에 {provider}-{YYYY-MM}/{provider}-{YYYY-MM-DD}.parquet

zip 구성은 utility/montly_share_raw_dataset_generator 가 만드는
share-raw-dataset 의 월별 zip 과 같은 방식이다. 달 이름 폴더를 최상단에 두고
그 아래에 파일을 담는다.

Lambda 가 아니라 컨테이너로 도는 이유는 시간이다. azure 는 하루치를 펼치는 데
7분쯤 걸려서 한 달이면 세 시간이 넘는다. Lambda 의 15분 제한으로는 한 달을
한 번에 묶을 수 없다. 기존 생성기도 같은 이유로 컨테이너다.

이미 Drive 에 있는 달은 건너뛴다. 그래서 중간에 끊겨도 다시 실행하면 남은
달부터 이어서 한다.

환경변수:
    DRIVE_SECRET_NAME   Secrets Manager 시크릿 이름 (필수)
    DRIVE_FOLDER_ID     업로드 대상 Drive 폴더 ID (필수)
    RAW_BUCKET          기본 spotlake
    BATCH_PKS           한 번에 정렬할 항목 수, 기본 20000
    ONLY_PROVIDER       aws|azure|gcp 중 하나만 처리
    ONLY_MONTH          YYYY-MM 한 달만 처리
    DRY_RUN             "1"이면 zip 까지만 만들고 Drive 는 건드리지 않는다
    KEEP_WORK           "1"이면 중간 파일을 지우지 않는다 (디버깅용)
"""

import calendar
import gzip
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import polars as pl
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

RAW_BUCKET = os.environ.get("RAW_BUCKET", "spotlake")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")
DRIVE_SECRET_NAME = os.environ.get("DRIVE_SECRET_NAME", "")
BATCH_PKS = int(os.environ.get("BATCH_PKS", "20000"))
DOWNLOAD_WORKERS = int(os.environ.get("DOWNLOAD_WORKERS", "16"))
ONLY_PROVIDER = os.environ.get("ONLY_PROVIDER", "").strip().lower()
ONLY_MONTH = os.environ.get("ONLY_MONTH", "").strip()
DRY_RUN = os.environ.get("DRY_RUN") == "1"
KEEP_WORK = os.environ.get("KEEP_WORK") == "1"

WINDOW_NEW = 6    # 0.5년: 이만큼 지난 달부터 공개
WINDOW_OLD = 18   # 1.5년: 이만큼 지나면 내린다

WORK = Path(os.environ.get("WORK_DIR", "/tmp/work"))

PK_COLUMNS = {
    "aws": ["InstanceType", "Region", "AZ"],
    "azure": ["InstanceTier", "InstanceType", "Region", "AZ"],
    "gcp": ["InstanceType", "Region"],
}

# 원본 Rawdata의 값 컬럼. 없는 것은 null로 채워 시기별 스키마 차이를 흡수한다.
VALUE_ORDER = {
    "aws": ["SPS", "T3", "T2", "IF", "OndemandPrice", "SpotPrice", "Savings"],
    "azure": ["Score", "T3", "T2", "IF", "DesiredCount",
              "OndemandPrice", "SpotPrice", "Savings"],
    "gcp": ["OndemandPrice", "SpotPrice", "Savings"],
}

# Azure Score는 과거의 Low/High/Restricted... 문자열과 이후 숫자를 모두 보존한다.
VALUE_DTYPES = {
    provider: {c: pl.String if provider == "azure" and c == "Score" else pl.Float64
               for c in columns}
    for provider, columns in VALUE_ORDER.items()
}

# 시기에 따라 이름이 다른 컬럼들. 값은 같다.
RENAME = {
    "AvailabilityZone": "AZ",            # azure 전 구간
    "OnDemand Price": "OndemandPrice",   # gcp
    "Spot Price": "SpotPrice",           # gcp
}

# 관측 시각과 무관하거나 내부용인 컬럼. Time은 키에서 다시 만든다.
DROP = {"id", "SPS_Update_Time", "Time"}

# 기존 생성기와 같이 리전을 박아 둔다. 실행 환경의 설정에 기대지 않는다.
session = boto3.session.Session(region_name="us-west-2")
# 기본 연결 풀은 10개뿐이라 받는 스레드를 늘려도 거기서 막힌다.
s3 = session.client("s3", config=Config(
    max_pool_connections=DOWNLOAD_WORKERS + 4,
    connect_timeout=15, read_timeout=60,
    retries={"max_attempts": 5, "mode": "standard"}))


# --------------------------------------------------------------------------
# 달 계산
# --------------------------------------------------------------------------

def add_months(year, month, delta):
    n = year * 12 + (month - 1) + delta
    return n // 12, n % 12 + 1


def window_months(today):
    """오늘 기준 공개 대상인 (year, month) 목록. 오래된 것부터."""
    lo = add_months(today.year, today.month, -WINDOW_OLD)
    hi = add_months(today.year, today.month, -WINDOW_NEW)
    out, cur = [], lo
    while cur <= hi:
        out.append(cur)
        cur = add_months(cur[0], cur[1], 1)
    return out


def label(year, month):
    return f"{year}-{month:02d}"


# --------------------------------------------------------------------------
# Google Drive (urllib만 사용)
# --------------------------------------------------------------------------

FILES_URL = "https://www.googleapis.com/drive/v3/files"
UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"
TOKEN_URL = "https://oauth2.googleapis.com/token"
FOLDER_MIME = "application/vnd.google-apps.folder"
CHUNK = 8 * 1024 * 1024  # resumable 업로드 조각. 256KB의 배수여야 한다.

_token = None  # (access_token, 만료 epoch)


def _http(url, method="GET", data=None, headers=None, timeout=300):
    """헤더는 dict로 바꾸지 않고 그대로 돌려준다. HTTP 헤더 이름은
    대소문자를 가리지 않는데 dict로 만들면 가리게 된다."""
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.headers, r.read()


def _access_token():
    global _token
    if _token and _token[1] > time.time() + 60:
        return _token[0]
    if not DRIVE_SECRET_NAME:
        raise RuntimeError(
            "DRIVE_SECRET_NAME 환경변수가 필요합니다. DRY_RUN=1 이면 없어도 됩니다.")

    raw = session.client("secretsmanager").get_secret_value(
        SecretId=DRIVE_SECRET_NAME)["SecretString"]
    c = json.loads(raw)
    missing = [k for k in ("client_id", "client_secret", "refresh_token")
               if not c.get(k)]
    if missing:
        raise RuntimeError(f"시크릿에 다음 항목이 없습니다: {', '.join(missing)}")

    body = urllib.parse.urlencode({
        "client_id": c["client_id"],
        "client_secret": c["client_secret"],
        "refresh_token": c["refresh_token"],
        "grant_type": "refresh_token",
    }).encode()
    _, _, resp = _http(TOKEN_URL, "POST", body,
                       {"Content-Type": "application/x-www-form-urlencoded"})
    tok = json.loads(resp)
    _token = (tok["access_token"], time.time() + tok.get("expires_in", 3600))
    return _token[0]


def _auth():
    return {"Authorization": f"Bearer {_access_token()}"}


def drive_children(parent_id, name=None):
    """폴더 바로 아래 항목 목록. name을 주면 그 이름만."""
    q = f"'{parent_id}' in parents and trashed = false"
    if name:
        q += " and name = '{}'".format(name.replace("'", "\\'"))
    out, page = [], None
    while True:
        params = {"q": q, "fields": "files(id,name,mimeType),nextPageToken",
                  "pageSize": "1000", "supportsAllDrives": "true",
                  "includeItemsFromAllDrives": "true"}
        if page:
            params["pageToken"] = page
        _, _, resp = _http(f"{FILES_URL}?{urllib.parse.urlencode(params)}",
                           headers=_auth())
        body = json.loads(resp)
        out.extend(body.get("files", []))
        page = body.get("nextPageToken")
        if not page:
            return out


def drive_folder(parent_id, name):
    """이름이 같은 하위 폴더를 찾고, 없으면 만든다."""
    for f in drive_children(parent_id, name):
        if f["mimeType"] == FOLDER_MIME:
            return f["id"]
    meta = json.dumps({"name": name, "parents": [parent_id],
                       "mimeType": FOLDER_MIME}).encode()
    _, _, resp = _http(f"{FILES_URL}?supportsAllDrives=true", "POST", meta,
                       {**_auth(), "Content-Type": "application/json"})
    return json.loads(resp)["id"]


def drive_upload(parent_id, name, path):
    """resumable 업로드.

    월별 zip 은 수백 MB 라 한 번에 올리지 않고 조각으로 나눈다. 통째로
    메모리에 올리지 않아도 되고, 중간에 끊겨도 그 조각만 다시 보내면 된다.
    """
    meta = json.dumps({"name": name, "parents": [parent_id]}).encode()
    _, headers, _ = _http(f"{UPLOAD_URL}&supportsAllDrives=true", "POST", meta,
                          {**_auth(), "Content-Type": "application/json"})
    session = headers.get("Location")
    if not session:
        raise RuntimeError(f"resumable 업로드 세션 URI를 받지 못했습니다: {name}")

    total = path.stat().st_size
    with path.open("rb") as fh:
        sent = 0
        while sent < total:
            block = fh.read(CHUNK)
            end = sent + len(block) - 1
            req = urllib.request.Request(
                session, data=block, method="PUT",
                headers={"Content-Length": str(len(block)),
                         "Content-Range": f"bytes {sent}-{end}/{total}"})
            try:
                with urllib.request.urlopen(req, timeout=600):
                    pass
            except urllib.error.HTTPError as e:
                # 조각이 더 남았으면 308 로 답한다. 오류가 아니다.
                if e.code != 308:
                    raise
            sent = end + 1


def drive_delete(file_id):
    _http(f"{FILES_URL}/{file_id}?supportsAllDrives=true", "DELETE",
          headers=_auth())


# --------------------------------------------------------------------------
# Rawdata 읽기
# --------------------------------------------------------------------------

TICK_RE = re.compile(r"/(\d{2})-(\d{2})-(\d{2})\.csv\.gz$")


def raw_keys(provider, year, month, day):
    """그날 rawdata 키를 시각 순으로 돌려준다. 없으면 빈 목록."""
    prefix = f"rawdata/{provider}/{year:04d}/{month:02d}/{day:02d}/"
    keys, token = [], None
    while True:
        kw = {"Bucket": RAW_BUCKET, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", [])
                 if TICK_RE.search(o["Key"])]
        if not page.get("IsTruncated"):
            return sorted(keys)
        token = page["NextContinuationToken"]


def tick_of(key, year, month, day):
    """관측 시각은 키에서 만든다.

    CSV 안의 시각 컬럼은 시기마다 이름이 다르고(Time / SPS_Update_Time)
    2025년 azure에는 관측 시각이 아예 없다. 키의 디렉터리와 파일명은
    전 구간 같은 규칙이라 이쪽이 유일하게 일관된 출처다.
    """
    m = TICK_RE.search(key)
    h, mi, s = map(int, m.groups())
    return datetime(year, month, day) + timedelta(hours=h, minutes=mi,
                                                  seconds=s)


def normalize(df, provider, tick):
    """시기별 스키마 차이를 흡수해 Time + PK + 값 컬럼 순으로 맞춘다."""
    df = df.rename({k: v for k, v in RENAME.items() if k in df.columns})
    df = df.drop([c for c in DROP if c in df.columns])

    pk = PK_COLUMNS[provider]
    values = VALUE_ORDER[provider]
    # CSV마다 정수/소수 추론 결과가 다르므로 기존 값도 같은 자료형으로 맞춘다.
    # 예: Azure T2/T3 추가 전후, IF가 소수에서 -1만 있는 시점으로 바뀔 때.
    # 변환할 수 없는 값은 오류로 남겨 데이터 손실을 숨기지 않는다.
    typed = [pl.col(c).cast(dtype) if c in df.columns
             else pl.lit(None, dtype=dtype).alias(c)
             for c, dtype in VALUE_DTYPES[provider].items()]
    df = df.with_columns(typed + [
        pl.lit(tick, dtype=pl.Datetime("us")).alias("Time")])

    # 아는 컬럼을 앞에 두고, 나중에 늘어난 컬럼이 있으면 뒤에 붙여 살린다.
    known = set(["Time"] + pk + values)
    extra = [c for c in df.columns if c not in known]
    return df.select(["Time"] + pk + values + extra)


def fetch(key, tries=5):
    """한 시점을 받는다. 끊기면 다시 받는다.

    한 달이면 수천 번 받는데 그중 한 번만 끊겨도 잡 전체가 죽는다. 실제로
    2025-06 백필이 읽기 도중 타임아웃으로 중단됐다. botocore 의 재시도는
    본문을 읽는 중에 끊기는 것까지는 덮지 못해서 여기서 다시 받는다.
    없는 키나 권한 문제는 다시 받아도 같으므로 바로 올린다.
    """
    for n in range(tries):
        try:
            return key, s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
        except ClientError:
            raise
        except Exception:
            if n == tries - 1:
                raise
            time.sleep(2 ** n)


def parse_tick(key, body, provider, year, month, day):
    df = pl.read_csv(gzip.decompress(body), infer_schema_length=None)
    return normalize(df, provider, tick_of(key, year, month, day))


def _collect(lf):
    """polars 버전에 따라 스트리밍 인자 이름이 다르다."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)


def build_day(provider, year, month, day, path):
    """그날 rawdata 전부를 PK 순으로 정렬한 parquet 한 개로 만든다.

    정렬이 핵심이다. 같은 항목의 값이 붙어 있어야 압축이 먹는다. aws
    하루치 기준으로 PK 순은 2.78 MB, 시각 순은 21.56 MB, 정렬 없이는
    28.40 MB다.

    하루치를 통째로 메모리에 올리면 azure가 10 GB를 넘긴다. 그래서 세
    단계로 나눈다. 시점별로 정규화해 작업 폴더에 떨구고, PK 목록을 만들고,
    PK 묶음별로 모아 정렬해 이어붙인다. 최대 메모리는 날짜나 벤더가
    아니라 BATCH_PKS가 정한다.
    """
    keys = raw_keys(provider, year, month, day)
    if not keys:
        return 0, 0

    pk = PK_COLUMNS[provider]
    tmp = path.parent / f".{path.stem}.parts"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        # 1) 시점 하나씩 파싱해 떨군다. 메모리에는 그 시점 하나만 남는다.
        #
        # 받는 것만 병렬로 한다. 시점 하나가 1MB 남짓인데 왕복 지연 때문에
        # 하나씩 받으면 azure 하루치 133초 중 108초가 여기서 갔다. 파싱과
        # 쓰기는 그대로 순서대로 해서 메모리는 예전과 같이 둔다.
        universe = None
        ticks = []
        window = DOWNLOAD_WORKERS * 2
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
            for i in range(0, len(keys), window):
                # 한 번에 다 띄우지 않고 창 단위로 끊는다. 받아 둔 압축
                # 바이트가 메모리에 쌓이는 양을 이걸로 묶어 둔다.
                for key, body in pool.map(fetch, keys[i:i + window]):
                    df = parse_tick(key, body, provider, year, month, day)
                    part = tmp / f"t{len(ticks):05d}.parquet"
                    df.write_parquet(part, compression="zstd")
                    ticks.append(part)
                    seen = df.select(pk).unique()
                    universe = seen if universe is None else pl.concat(
                        [universe, seen]).unique()
                    del df, body

        # 2) PK 순서를 정해 묶음으로 나눈다.
        universe = universe.sort(pk)

        # 3) 묶음별로 모아 정렬한다. 각 묶음이 이미 최종 순서라 뒤에서
        #    이어붙이기만 하면 전체가 정렬된 상태가 된다.
        chunks, rows = [], 0
        for i in range(0, universe.height, BATCH_PKS):
            sel = universe.slice(i, BATCH_PKS)
            df = _collect(pl.scan_parquet(ticks).join(sel.lazy(), on=pk,
                                                      how="semi"))
            if df.is_empty():
                continue
            # 원본에는 같은 PK와 시각이 두 번 들어간 파일이 있다. 값이
            # 같으므로 정렬한 뒤 앞의 것만 남긴다.
            df = (df.sort(pk + ["Time"])
                    .unique(subset=pk + ["Time"], keep="first",
                            maintain_order=True))
            chunk = tmp / f"c{len(chunks):05d}.parquet"
            df.write_parquet(chunk, compression="zstd")
            chunks.append(chunk)
            rows += df.height
            del df

        if not chunks:
            return 0, 0
        pl.scan_parquet(chunks).sink_parquet(path, compression="zstd")
        return rows, len(keys)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------
# 월별 zip 만들기
# --------------------------------------------------------------------------

def build_month(provider, year, month):
    """그 달 날짜별 parquet을 만들어 zip 하나로 묶는다. zip 경로를 돌려준다.

    zip 안은 share-raw-dataset 과 같은 방식으로 달 이름 폴더를 최상단에
    둔다. parquet 은 이미 압축되어 있어서 다시 압축해도 줄지 않으므로
    ZIP_STORED 로 담기만 한다. 그만큼 빠르다.
    """
    lab = label(year, month)
    stem = f"{provider}-{lab}"
    days = calendar.monthrange(year, month)[1]

    day_dir = WORK / stem
    shutil.rmtree(day_dir, ignore_errors=True)
    day_dir.mkdir(parents=True, exist_ok=True)

    made, total_rows, missing = [], 0, []
    for day in range(1, days + 1):
        name = f"{stem}-{day:02d}.parquet"
        path = day_dir / name
        started = time.time()
        rows, ticks = build_day(provider, year, month, day, path)
        if not rows:
            missing.append(day)
            print(f"    {name}: 원본 없음")
            continue
        made.append(path)
        total_rows += rows
        print(f"    {name}: {rows:,}행 {ticks}시점 "
              f"{path.stat().st_size / 1024**2:.2f} MB "
              f"{time.time() - started:.0f}초", flush=True)

    if not made:
        shutil.rmtree(day_dir, ignore_errors=True)
        return None, 0, missing

    zip_path = WORK / f"{stem}.zip"
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as z:
        for p in made:
            z.write(p, arcname=f"{stem}/{p.name}")

    if not KEEP_WORK:
        shutil.rmtree(day_dir, ignore_errors=True)
    print(f"  {zip_path.name}: {len(made)}일 {total_rows:,}행 "
          f"{zip_path.stat().st_size / 1024**2:.1f} MB")
    return zip_path, total_rows, missing


# --------------------------------------------------------------------------
# 동기화
# --------------------------------------------------------------------------

def sync(report):
    today = datetime.now(timezone.utc).date()
    months = window_months(today)
    if ONLY_MONTH:
        y, m = map(int, ONLY_MONTH.split("-"))
        months = [(y, m)]
    providers = [ONLY_PROVIDER] if ONLY_PROVIDER else sorted(PK_COLUMNS)

    for provider in providers:
        if provider not in PK_COLUMNS:
            raise SystemExit(f"알 수 없는 벤더: {provider}")
        folder = None if DRY_RUN else drive_folder(DRIVE_FOLDER_ID, provider)
        existing = set() if DRY_RUN else {f["name"] for f in drive_children(folder)}

        for year, month in months:
            name = f"{provider}-{label(year, month)}.zip"
            if name in existing:
                print(f"{name}: 이미 있음, 건너뜀")
                report["skipped"].append(name)
                continue

            print(f"{name}: 만드는 중", flush=True)
            started = time.time()
            zip_path, rows, missing = build_month(provider, year, month)
            if zip_path is None:
                print(f"{name}: 그 달 원본이 없어 건너뜁니다")
                report["skipped"].append(f"{name} (원본 없음)")
                continue

            if not DRY_RUN:
                drive_upload(folder, name, zip_path)
            if not KEEP_WORK:
                zip_path.unlink(missing_ok=True)
            print(f"{name}: 완료 {time.time() - started:.0f}초"
                  f"{' (DRY_RUN, 업로드 안 함)' if DRY_RUN else ''}\n", flush=True)
            report["uploaded"].append(
                {"name": name, "rows": rows, "missing_days": missing})


def prune(report):
    """창을 벗어난 zip을 Drive에서 지운다."""
    if DRY_RUN:
        return
    today = datetime.now(timezone.utc).date()
    keep = {f"{p}-{label(y, m)}.zip"
            for p in PK_COLUMNS for y, m in window_months(today)}
    for prov in drive_children(DRIVE_FOLDER_ID):
        if prov["mimeType"] != FOLDER_MIME or prov["name"] not in PK_COLUMNS:
            continue
        for f in drive_children(prov["id"]):
            if f["mimeType"] == FOLDER_MIME or not f["name"].endswith(".zip"):
                continue
            if f["name"] not in keep:
                drive_delete(f["id"])
                report["deleted"].append(f["name"])
                print(f"창을 벗어나 삭제: {f['name']}")


def main():
    if not DRIVE_FOLDER_ID and not DRY_RUN:
        raise SystemExit("DRIVE_FOLDER_ID 환경변수가 필요합니다.")

    today = datetime.now(timezone.utc).date()
    report = {"window": [label(y, m) for y, m in window_months(today)],
              "uploaded": [], "deleted": [], "skipped": []}
    print(f"창: {report['window'][0]} ~ {report['window'][-1]}"
          f" ({len(report['window'])}개월)\n")

    WORK.mkdir(parents=True, exist_ok=True)
    try:
        sync(report)
        # 범위를 좁혀 돌린 실행에서는 정리하지 않는다. 창 전체를 보지
        # 않았으므로 무엇을 지워야 할지 판단할 수 없다.
        if not ONLY_PROVIDER and not ONLY_MONTH:
            prune(report)
    finally:
        if not KEEP_WORK:
            shutil.rmtree(WORK, ignore_errors=True)

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    sys.exit(main())
