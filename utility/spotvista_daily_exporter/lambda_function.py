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
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
from itertools import groupby

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs

SRC_BUCKET = os.environ.get("SRC_BUCKET", "titans-spotlake-data")
DST_BUCKET = os.environ.get("DST_BUCKET", "spotlake-public-daily")
DST_PREFIX = os.environ.get("DST_PREFIX", "spotvista/aws")
WARM_PREFIX = "parquet_warm/aws/m8"
FIRST_DAY = date(2025, 2, 15)   # TITANS에 T3가 처음 나오는 날
TICKS = 144                      # 하루의 10분 칸 수
LOOKBACK = 7                     # 빠진 날을 다시 채우는 범위

# 시기마다 없는 컬럼이 있다. T3는 2025-02 중순부터, Ceased는 그 뒤에 생겼다.
COLUMNS = {
    "InstanceType": pa.string(), "Region": pa.string(), "AZ": pa.string(),
    "Time": pa.timestamp("us", "UTC"), "SPS": pa.int64(), "T3": pa.int64(),
    "SpotPrice": pa.float64(), "Ceased": pa.bool_(),
}

_clients = {}


def _s3():
    if "s3" not in _clients:
        _clients["s3"] = boto3.client("s3")
        _clients["fs"] = fs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-west-2"))
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


def _read(key):
    """파일 하나를 COLUMNS 모양으로 읽는다. 없는 컬럼은 null로 채운다."""
    path = f"{SRC_BUCKET}/{key}"
    names = pq.read_schema(path, filesystem=_clients["fs"]).names
    t = pq.read_table(path, columns=[c for c in COLUMNS if c in names],
                      filesystem=_clients["fs"])
    return pa.table({c: t[c].cast(ty) if c in names else pa.nulls(len(t), ty)
                     for c, ty in COLUMNS.items()})


def _codes(col):
    col = pc.fill_null(col, "")
    values = pc.unique(col)
    return pc.index_in(col, value_set=values).to_numpy().astype(np.int64), values.to_pylist()


def load_month(month, until):
    """그 달 변경점을 (항목, 시각) 순으로 정렬한 배열로 돌려준다.

    Ceased 행은 값이 0으로 기록돼 있으므로 NaN으로 바꾼다. 그 항목은 다음 정상 행
    전까지 없는 것으로 본다.
    """
    _s3()
    t = pa.concat_tables([_read(k) for k in month_files(month, until)])
    t = t.filter(pc.less_equal(t["Time"], pa.scalar(until, COLUMNS["Time"])))

    it, it_names = _codes(t["InstanceType"])
    rg, rg_names = _codes(t["Region"])
    az, az_names = _codes(t["AZ"])
    raw = (it * len(rg_names) + rg) * len(az_names) + az
    uniq, code = np.unique(raw, return_inverse=True)
    names = [(it_names[u // (len(rg_names) * len(az_names))],
              rg_names[u // len(az_names) % len(rg_names)],
              az_names[u % len(az_names)]) for u in uniq.tolist()]

    minute = t["Time"].cast(pa.int64()).to_numpy() // 60_000_000
    ceased = t["Ceased"].fill_null(False).to_numpy(zero_copy_only=False)
    vals = {}
    for c in ("T3", "SPS", "SpotPrice"):
        # to_numpy는 null이 없으면 읽기 전용 배열을 준다. 고치지 않고 새로 만든다.
        vals[c] = np.where(ceased, np.nan, t[c].cast(pa.float64()).to_numpy(zero_copy_only=False))
    del t

    order = np.lexsort((minute, code))
    code, minute = code[order], minute[order]
    vals = {c: v[order] for c, v in vals.items()}
    # 같은 항목·시각이 두 번 있으면 뒤의 것을 쓴다.
    keep = np.r_[(code[1:] != code[:-1]) | (minute[1:] != minute[:-1]), True]
    return {"names": names, "code": code[keep], "minute": minute[keep],
            **{c: v[keep] for c, v in vals.items()}}


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
    with ThreadPoolExecutor(32) as pool:
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
        try:
            data = load_month(month, until)
        except NotReady as e:
            print(f"skip: {e}")
            continue
        for day in group:
            files = day_files(data, day, summarize_day(data, day))
            write_day(day, files)
            update_index(day, files)
            made.append(f"{day}")
            print(f"{day}: {len(files)} files")
        del data
    return {"days": made}
