"""SpotVista가 쓰는 일별 T3 합계를 만든다.

TITANS 변경점 parquet으로 하루치 10분 격자를 복원해 (InstanceType, Region, AZ)마다
T3 합계 6개와 그날 마지막 값을 공개 버킷에 쓴다. SpotVista 브라우저는 7일치 합계를
이어 붙여 기존 쿼리 모듈(calculate_score)과 같은 점수를 계산한다. 10분 단위 곡선은
공개하지 않는다.

    s3://titans-spotlake-data/aws/YYYY-MM_AP.parquet          월초 상태
    s3://titans-spotlake-data/aws/YYYY-MM.parquet             지난 달 변경점
    s3://titans-spotlake-data/parquet_warm/aws/m8/YYYY/MM/manifest.json
                                                              이번 달 변경점 파일 목록
        -> s3://spotlake-public-daily/spotvista/aws/daily/YYYY-MM-DD/{InstanceType}.json
           s3://spotlake-public-daily/spotvista/aws/index.json

이벤트:
    {}                        어제(UTC)와, 최근 7일 중 아직 만들지 않은 날
    {"date": "2026-10-11"}    그 날 하나를 다시 만든다
    {"month": "2025-03"}      그 달 전체를 만든다 (백필)
"""
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
from itertools import groupby

import boto3
import duckdb
import numpy as np
from botocore.config import Config

SRC_BUCKET = os.environ.get("SRC_BUCKET", "titans-spotlake-data")
DST_BUCKET = os.environ.get("DST_BUCKET", "spotlake-public-daily")
DST_PREFIX = os.environ.get("DST_PREFIX", "spotvista/aws")
WARM_PREFIX = "parquet_warm/aws/m8"
FIRST_DAY = date(2025, 2, 15)   # TITANS에 T3가 처음 나오는 날
TICKS = 144                      # 하루의 10분 칸 수
LOOKBACK = 7                     # 빠진 날을 다시 채우는 범위
WRITERS = 32                     # 하루 약 1,400개 파일을 동시에 쓰는 수
VALUES = ("T3", "SPS", "SpotPrice")

_clients = {}


def _s3():
    if "s3" not in _clients:
        # 기본 연결 풀(10개)로는 WRITERS개 스레드가 10개씩만 나간다.
        _clients["s3"] = boto3.client("s3", config=Config(max_pool_connections=WRITERS))
    return _clients["s3"]


class NotReady(Exception):
    """TITANS가 아직 그 시각까지 처리하지 않았다. 다음 실행이 다시 시도한다."""


def _exists(bucket, key):
    # head_object는 목록 권한이 접두사로 제한돼 있으면 없는 키에 403을 준다. 목록으로 확인한다.
    found = _s3().list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1).get("Contents", [])
    return bool(found) and found[0]["Key"] == key


# ------------------------------------------------------------------ 읽기

def month_files(month, until):
    """그 달 하루치를 복원하는 데 필요한 TITANS 키 목록.

    지난 달은 월 파일 하나, 이번 달은 manifest에 적힌 Warm·Hot 파일이다. 둘 다 앞에
    월초 상태(앵커)를 붙인다. manifest가 until까지 처리하지 않았으면 NotReady.
    """
    ym = f"{month:%Y-%m}"
    keys = [f"aws/{ym}_AP.parquet"]
    if _exists(SRC_BUCKET, f"aws/{ym}.parquet"):
        return keys + [f"aws/{ym}.parquet"]

    warm = f"{WARM_PREFIX}/{month:%Y/%m}"
    body = _s3().get_object(Bucket=SRC_BUCKET, Key=f"{warm}/manifest.json")["Body"].read()
    manifest = json.loads(body)
    done = datetime.fromisoformat(manifest["last_processed_time"])
    if done < until:
        raise NotReady(f"{ym}: TITANS processed up to {done}, need {until}")
    # level 0 은 Hot 파일의 전체 키, 1 이상은 Warm 접두사 기준 이름이다.
    for level, entries in manifest["levels"].items():
        keys += [e["file"] if level == "0" else f"{warm}/{e['file']}" for e in entries]
    return keys


def open_month(month, until, tmp):
    """그 달 TITANS 파일을 tmp에 받아 DuckDB에 올린다. (연결, 항목 이름 목록)을 돌려준다.

    TITANS parquet은 zstd로 압축돼 있어 DuckDB로 읽는다. 한 달치(약 1,500만 행)를
    문자열 그대로 정렬하면 메모리를 넘으므로, 항목 목록(약 5만 개)으로 번호를 매기고
    변경점에는 번호만 붙여 표 c에 둔다. 날짜별로 필요한 행은 day_data가 꺼낸다.

    시기마다 없는 컬럼(T3, Ceased)은 NULL이다. Ceased 행은 값이 0으로 기록돼 있으므로
    NULL로 바꾼다. 그 항목은 다음 정상 행 전까지 없는 것으로 본다.
    """
    paths = []
    for i, key in enumerate(month_files(month, until)):
        # 이름의 순번은 같은 항목·시각이 여러 파일에 있을 때 뒤 파일 것을 고르는 데 쓴다.
        paths.append(os.path.join(tmp, f"{i:04d}.parquet").replace("\\", "/"))
        _s3().download_file(SRC_BUCKET, key, paths[-1])

    con = duckdb.connect()
    con.execute(f"SET TimeZone='UTC'; SET temp_directory='{tmp}/spill'; SET memory_limit='3GB'; "
                "SET threads=4; SET preserve_insertion_order=false")
    files = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
    src = f"read_parquet({files}, union_by_name=true, filename=true)"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
    ceased = "coalesce(Ceased, false)" if "Ceased" in cols else "false"
    vals = ", ".join(f"CASE WHEN {ceased} THEN NULL ELSE {c} END::DOUBLE AS {c}" if c in cols
                     else f"NULL::DOUBLE AS {c}" for c in VALUES)
    minute = "epoch_us(Time) // 60000000"
    con.execute(f"""
        CREATE TEMP TABLE k AS
        SELECT row_number() OVER (ORDER BY InstanceType, Region, AZ) - 1 AS code, *
        FROM (SELECT DISTINCT InstanceType, Region, AZ FROM {src} WHERE Time <= $until)
    """, {"until": until})
    con.execute(rf"""
        CREATE TEMP TABLE c AS
        SELECT code, {minute} AS minute,
               {minute} * 10000 + regexp_extract(filename, '(\d+)\.parquet$', 1)::BIGINT AS ord,
               {vals}
        FROM {src} JOIN k USING (InstanceType, Region, AZ)
        WHERE Time <= $until
    """, {"until": until})
    names = [tuple(r) for r in con.execute("SELECT InstanceType, Region, AZ FROM k ORDER BY code").fetchall()]
    return con, names


def day_data(con, names, day):
    """그날 계산에 필요한 행: 항목마다 00:00 이하의 마지막 행과 그날의 변경점.

    (항목, 시각) 순으로 정렬한 배열로 돌려준다. 같은 항목·시각이 둘이면 뒤 파일 것을 쓴다.
    """
    d0 = int(datetime.combine(day, time(), timezone.utc).timestamp()) // 60
    fields = ", ".join(f"struct_extract(s, '{f}') AS {f}" for f in ("minute", "ord") + VALUES)
    arrays = con.execute(f"""
        SELECT code, {fields} FROM (
            SELECT code, arg_max(struct_pack(minute, ord, {', '.join(VALUES)}), ord) AS s
            FROM c WHERE minute <= {d0} GROUP BY code)
        UNION ALL
        SELECT code, minute, ord, {', '.join(VALUES)} FROM c
        WHERE minute > {d0} AND minute < {d0 + TICKS * 10}
        ORDER BY code, ord
    """).fetchnumpy()
    code = np.asarray(arrays["code"], dtype=np.int64)
    minute = np.asarray(arrays["minute"], dtype=np.int64)
    keep = np.r_[(code[1:] != code[:-1]) | (minute[1:] != minute[:-1]), True]
    # NULL이 있는 컬럼은 masked array로 온다. NaN으로 채운다.
    return {"names": names, "code": code[keep], "minute": minute[keep],
            **{c: np.ma.filled(np.ma.asarray(arrays[c], dtype=float), np.nan)[keep] for c in VALUES}}


# ------------------------------------------------------------------ 계산

def summarize_day(data, day):
    """하루치 10분 격자를 복원해 항목별 T3 합계와 마지막 값을 구한다.

    0번 칸(00:00)은 그 시각 이하의 마지막 행, 1~143번 칸은 그날의 변경점이고 그
    사이는 앞 값으로 이어 채운다. 다음 날 00:00 행은 다음 날 몫이다. NaN은 아직
    나타나지 않았거나 Ceased된 상태이며 그대로 이어진다.

    합계는 NaN을 뺀 값 y의 개수 n, 합, 제곱합, 위치 가중합 Σ i·y(i는 그날 안에서
    NaN을 뺀 순번), 첫 값, 끝 값이다. 브라우저는 7일치를 이어 붙여 원래 계산과 같은
    면적, 표준편차, 기울기를 얻는다.
    """
    code, minute, n_pk = data["code"], data["minute"], len(data["names"])
    d0 = int(datetime.combine(day, time(), timezone.utc).timestamp()) // 60

    before = minute <= d0
    # 정렬돼 있으므로 항목마다 d0 이하의 마지막 행은 다음 행이 다른 항목이거나 d0 이후인 행이다.
    carry = before & np.r_[(code[1:] != code[:-1]) | ~before[1:], True]
    rows = carry | ((minute > d0) & (minute < d0 + TICKS * 10))
    c = code[rows]
    tick = np.where(carry[rows], 0, (minute[rows] - d0) // 10)
    # 10분 경계에 맞지 않는 행이 한 칸에 겹치면 뒤의 것을 쓴다.
    keep = np.r_[(c[1:] != c[:-1]) | (tick[1:] != tick[:-1]), True]
    at = (c[keep], tick[keep])

    filled = np.zeros((n_pk, TICKS), bool)
    filled[at] = True
    pos = np.maximum.accumulate(np.where(filled, np.arange(TICKS), -1), axis=1)
    r = np.arange(n_pk)[:, None]

    def grid(name):
        g = np.full((n_pk, TICKS), np.nan)
        g[at] = data[name][rows][keep]
        g = g[r, np.maximum(pos, 0)]
        g[pos < 0] = np.nan
        return g

    y, sps, price = grid("T3"), grid("SPS"), grid("SpotPrice")
    valid = ~np.isnan(y)
    yz = np.where(valid, y, 0.0)
    i = np.where(valid, np.cumsum(valid, axis=1) - 1, 0)
    ar = np.arange(n_pk)
    first_i = valid.argmax(axis=1)
    last_i = TICKS - 1 - valid[:, ::-1].argmax(axis=1)
    paid = valid & (price > 0)
    price_i = TICKS - 1 - paid[:, ::-1].argmax(axis=1)
    return {
        "n": valid.sum(axis=1), "sum": yz.sum(axis=1), "sumsq": (yz * yz).sum(axis=1),
        "sum_iy": (i * yz).sum(axis=1), "first": y[ar, first_i], "last": y[ar, last_i],
        "last_i": last_i, "SPS": sps[ar, last_i],
        "SpotPrice": np.where(paid.any(axis=1), price[ar, price_i], np.nan),
    }


def day_files(data, day, s):
    """summarize_day 결과를 인스턴스 타입별 JSON 본문으로 묶는다. T3가 없는 항목은 뺀다."""
    d0 = datetime.combine(day, time())
    files = {}
    for k in np.flatnonzero(s["n"] > 0).tolist():
        itype, region, az = data["names"][k]
        price = s["SpotPrice"][k]
        files.setdefault(itype, []).append({
            "Region": region, "AZ": az,
            "T3": {"n": int(s["n"][k]), "sum": int(s["sum"][k]), "sumsq": int(s["sumsq"][k]),
                   "sum_iy": int(s["sum_iy"][k]), "first": int(s["first"][k]),
                   "last": int(s["last"][k])},
            "Last": {"Time": f"{d0 + timedelta(minutes=10 * int(s['last_i'][k])):%Y-%m-%d %H:%M:%S}",
                     "SPS": None if np.isnan(s["SPS"][k]) else int(s["SPS"][k]),
                     "T3": int(s["last"][k]),
                     "SpotPrice": None if np.isnan(price) else float(price)},
        })
    return {t: {"date": f"{day}", "InstanceType": t,
                "rows": sorted(rows, key=lambda x: (x["Region"], x["AZ"]))}
            for t, rows in files.items()}


# ------------------------------------------------------------------ 쓰기

def _put(key, body, max_age):
    _s3().put_object(Bucket=DST_BUCKET, Key=f"{DST_PREFIX}/{key}",
                     Body=json.dumps(body, separators=(",", ":")).encode(),
                     ContentType="application/json", CacheControl=f"public, max-age={max_age}")


def write_day(day, files):
    # 지난 날짜는 다시 만들 일이 드물다. 그래도 고쳐 쓸 수 있게 캐시는 하루로 둔다.
    with ThreadPoolExecutor(WRITERS) as pool:
        list(pool.map(lambda kv: _put(f"daily/{day}/{kv[0]}.json", kv[1], 86400), files.items()))
    # 마지막에 표시를 남긴다. 이것이 없는 날은 다음 실행이 다시 만든다.
    _put(f"daily/{day}/_done", {"files": len(files)}, 86400)


def update_index(day, files):
    key = f"{DST_PREFIX}/index.json"
    if _exists(DST_BUCKET, key):
        index = json.loads(_s3().get_object(Bucket=DST_BUCKET, Key=key)["Body"].read())
    else:
        index = {"dates": {"first": f"{day}", "last": f"{day}"}}
    dates = index["dates"]
    dates["first"] = min(dates["first"], f"{day}")
    if f"{day}" >= dates["last"] or "instance_types" not in index:
        dates["last"] = max(dates["last"], f"{day}")
        index["instance_types"] = {t: sorted({r["Region"] for r in f["rows"]})
                                   for t, f in sorted(files.items())}
    index["generated_at"] = f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}"
    _put("index.json", index, 300)


# ------------------------------------------------------------------ 진입점

def target_days(event, today):
    if "month" in event:
        m = date.fromisoformat(f"{event['month']}-01")
        days = [m + timedelta(d) for d in range(31) if (m + timedelta(d)).month == m.month]
    elif "date" in event:
        days = [date.fromisoformat(event["date"])]
    else:
        days = [today - timedelta(k) for k in range(LOOKBACK, 0, -1)]
        days = [d for d in days if not _exists(DST_BUCKET, f"{DST_PREFIX}/daily/{d}/_done")]
    return [d for d in days if FIRST_DAY <= d < today]


def lambda_handler(event, context):
    today = datetime.now(timezone.utc).date()
    made = []
    for month, group in groupby(target_days(event or {}, today), key=lambda d: d.replace(day=1)):
        group = list(group)
        until = datetime.combine(group[-1], time(23, 50), timezone.utc)
        # Lambda는 /tmp를 다음 호출에도 남기므로 받은 파일은 달마다 지운다.
        with tempfile.TemporaryDirectory() as tmp:
            try:
                con, names = open_month(month, until, tmp)
            except NotReady as e:
                print(f"skip: {e}")
                continue
            try:
                for day in group:
                    data = day_data(con, names, day)
                    files = day_files(data, day, summarize_day(data, day))
                    write_day(day, files)
                    update_index(day, files)
                    made.append(f"{day}")
                    print(f"{day}: {len(files)} files")
            finally:
                con.close()
    return {"days": made}
