"""SpotVista 추천에 쓰는 가용성 점수를 만든다.

TITANS 변경점 parquet으로 하루치 10분 격자를 복원해 (InstanceType, Region, AZ)마다
하루 T3 합계를 구하고, 기준일 전날까지 7일치를 이어 붙여 기존 쿼리 모듈
(calculate_score)과 같은 가용성 점수를 계산한다. 기준일마다 전체 인스턴스의 점수와
최근 값을 JSON 하나로 공개한다. SpotVista 브라우저는 이 파일 하나를 받아 고른 타입과
리전으로 거르고 비용 점수와 가중치만 적용한다. 10분 단위 곡선은 공개하지 않는다.

    s3://titans-spotlake-data/aws/YYYY-MM_AP.parquet          월초 상태
    s3://titans-spotlake-data/aws/YYYY-MM.parquet             지난 달 변경점
    s3://titans-spotlake-data/parquet_warm/aws/m8/YYYY/MM/manifest.json
                                                              이번 달 변경점 파일 목록
        -> s3://spotlake-public-daily/spotvista/aws/agg/YYYY-MM-DD.json     하루 T3 합계(중간 결과)
           s3://spotlake-public-daily/spotvista/aws/scores/YYYY-MM-DD.json  기준일의 점수
           s3://spotlake-public-daily/spotvista/aws/index.json

이벤트:
    {}                        어제(UTC)와, 최근 7일 중 아직 만들지 않은 날
    {"date": "2026-10-11"}    그 날 하나를 다시 만든다
    {"month": "2025-03"}      그 달 전체를 만든다 (백필)
"""
import json
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from itertools import groupby

import boto3
import duckdb
import numpy as np

SRC_BUCKET = os.environ.get("SRC_BUCKET", "titans-spotlake-data")
DST_BUCKET = os.environ.get("DST_BUCKET", "spotlake-public-daily")
DST_PREFIX = os.environ.get("DST_PREFIX", "spotvista/aws")
WARM_PREFIX = "parquet_warm/aws/m8"
FIRST_DAY = date(2025, 2, 15)   # TITANS에 T3가 처음 나오는 날
TICKS = 144                      # 하루의 10분 칸 수
WINDOW = 7                       # 기준일 전날까지 며칠을 보는지 (쿼리 모듈의 LOOKBACK_DAYS)
LOOKBACK = 7                     # 빠진 날을 다시 채우는 범위
VALUES = ("T3", "SPS", "SpotPrice")
AGG_FIELDS = ("n", "sum", "sumsq", "sum_iy", "first", "last")
SCORE_COLUMNS = ["AZ", "SPS", "T3", "SpotPrice", "AvailabilityScore"]

_clients = {}


def _s3():
    if "s3" not in _clients:
        _clients["s3"] = boto3.client("s3")
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

    ord는 같은 항목·시각의 행 중 무엇을 쓸지 정한다. warm_compactor처럼 Ceased가 아닌
    행을 먼저 쓰고, 그다음은 뒤 파일 것을 쓴다. 아직 합쳐지지 않은 Hot 파일에는 같은
    시각의 정상 행과 Ceased 행이 함께 있을 수 있다.
    """
    paths = []
    for i, key in enumerate(month_files(month, until)):
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
               {minute} * 100000 + CASE WHEN {ceased} THEN 0 ELSE 10000 END
                   + regexp_extract(filename, '(\d+)\.parquet$', 1)::BIGINT AS ord,
               {vals}
        FROM {src} JOIN k USING (InstanceType, Region, AZ)
        WHERE Time <= $until
    """, {"until": until})
    names = [tuple(r) for r in con.execute("SELECT InstanceType, Region, AZ FROM k ORDER BY code").fetchall()]
    return con, names


def day_data(con, names, day):
    """그날 계산에 필요한 행: 항목마다 00:00 이하의 마지막 행과 그날의 변경점.

    (항목, ord) 순으로 정렬한 배열로 돌려준다. 같은 항목·시각이 둘이면 ord가 큰 행을 쓴다.
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
    NaN을 뺀 순번), 첫 값, 끝 값이다. window_scores가 7일치를 이어 붙여 원래 계산과
    같은 면적, 표준편차, 기울기를 얻는다.
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


def day_agg(data, day, s):
    """summarize_day 결과 중 T3가 있는 항목을 열 단위로 묶는다. 점수 계산의 중간 결과다.

    SPS, SpotPrice는 그날 마지막 값이고(가격은 마지막으로 0보다 큰 값), T3의 마지막
    값은 last다. 없으면 None이다.
    """
    k = np.flatnonzero(s["n"] > 0)
    names = [data["names"][i] for i in k.tolist()]
    nullable = lambda a: [None if np.isnan(v) else v for v in a[k].tolist()]
    return {"date": f"{day}",
            "InstanceType": [n[0] for n in names], "Region": [n[1] for n in names],
            "AZ": [n[2] for n in names],
            **{f: s[f][k].astype(np.int64).tolist() for f in AGG_FIELDS},
            "SPS": nullable(s["SPS"]), "SpotPrice": nullable(s["SpotPrice"])}


@lru_cache(maxsize=None)
def _max_slope(n):
    # get_slope의 기준 직선: 0에서 50으로 오르는 정수 값. 원래 식 그대로 n마다 한 번 계산한다.
    return np.polyfit(np.linspace(0, n - 1, n), np.linspace(0, 50, n).astype(int), 1)[0]


def window_scores(aggs):
    """기준일 전날까지 7일의 하루 합계(오래된 날부터, 없는 날은 None)로 점수 파일 본문을 만든다.

    calculate_score.py를 합계로 옮긴 것이다. 날짜 순으로 이은 T3 값 y(x = 0..n-1)의
    사다리꼴 면적, 표준편차(ddof=1), 최소제곱 기울기는 개수·합·제곱합·Σ x·y·첫 값·끝 값으로
    정해진다. 날을 이으면 그날 순번 i는 앞선 개수 N만큼 밀리므로 Σ x·y에 N·합을 더한다.

    후보는 쿼리 모듈의 lambda_function.py처럼 창 안 가장 최근 값의 SPS와 가격이 0보다 큰
    항목이다. 값이 2개 미만이면 원래 식이 NaN이 되므로 뺀다.
    """
    keys = {}
    for a in filter(None, aggs):
        for key in zip(a["InstanceType"], a["Region"], a["AZ"]):
            keys.setdefault(key, len(keys))
    n, s, ss, sxy, first, last = (np.zeros(len(keys)) for _ in range(6))
    sps, price = np.full(len(keys), np.nan), np.full(len(keys), np.nan)
    for a in filter(None, aggs):
        i = np.array([keys[k] for k in zip(a["InstanceType"], a["Region"], a["AZ"])], dtype=np.int64)
        d = {f: np.array(a[f], dtype=float) for f in AGG_FIELDS}
        first[i] = np.where(n[i] == 0, d["first"], first[i])
        sxy[i] += n[i] * d["sum"] + d["sum_iy"]
        n[i] += d["n"]
        s[i] += d["sum"]
        ss[i] += d["sumsq"]
        last[i] = d["last"]
        sps[i] = np.array(a["SPS"], dtype=float)          # None은 NaN이 된다
        price[i] = np.array(a["SpotPrice"], dtype=float)

    ok = np.flatnonzero((n >= 2) & (sps > 0) & (price > 0))
    n, s, ss, sxy, first, last = (v[ok] for v in (n, s, ss, sxy, first, last))
    integral = (s - (first + last) / 2) / (50 * (n - 1))                 # trapezoid(y) / trapezoid([50]*n)
    std = np.sqrt(np.maximum(0, (ss - s * s / n) / (n - 1)))
    sx, sxx = n * (n - 1) / 2, (n - 1) * n * (2 * n - 1) / 6
    slope = (n * sxy - sx * s) / (n * sxx - sx * sx)                      # polyfit(x, y, 1)[0]
    # 원래 코드는 음수 기울기를 (a / -max) * -0.1 로 따로 계산하지만 값은 같다.
    slope = slope / np.array([_max_slope(int(v)) for v in n]) * 0.1
    score = (integral - integral * (std / 25 * 0.1) + integral * slope) * 100

    names = list(keys)
    rows = {}
    for j, sc, t3 in zip(ok.tolist(), score.tolist(), last.tolist()):
        itype, region, az = names[j]
        rows.setdefault(itype, {}).setdefault(region, []).append(
            # 점수는 소수 넷째 자리까지 둔다(차이 5e-5 미만). 파일 크기를 줄인다.
            [az, int(sps[j]), int(t3), float(price[j]), round(sc, 4)])
    return {itype: {r: sorted(rs) for r, rs in sorted(regions.items())}
            for itype, regions in sorted(rows.items())}


# ------------------------------------------------------------------ 쓰기

def _put(key, body, max_age):
    _s3().put_object(Bucket=DST_BUCKET, Key=f"{DST_PREFIX}/{key}",
                     Body=json.dumps(body, separators=(",", ":")).encode(),
                     ContentType="application/json", CacheControl=f"public, max-age={max_age}")


def load_agg(day, aggs):
    """그날 하루 합계. 이번 실행에서 만든 것(aggs)이 없으면 버킷에서 읽는다. 없으면 None."""
    if day not in aggs:
        key = f"{DST_PREFIX}/agg/{day}.json"
        aggs[day] = (json.loads(_s3().get_object(Bucket=DST_BUCKET, Key=key)["Body"].read())
                     if day >= FIRST_DAY and _exists(DST_BUCKET, key) else None)
    return aggs[day]


def update_index(ref, rows):
    """기준일 범위와 가장 최근 기준일의 InstanceType → Region 목록. 선택 목록에 쓴다."""
    key = f"{DST_PREFIX}/index.json"
    index = json.loads(_s3().get_object(Bucket=DST_BUCKET, Key=key)["Body"].read()) \
        if _exists(DST_BUCKET, key) else {"dates": {"last": f"{ref}"}}
    # 첫 기준일은 창 7일이 모두 T3가 있는 날부터다.
    index["dates"]["first"] = f"{FIRST_DAY + timedelta(WINDOW)}"
    if f"{ref}" >= index["dates"]["last"] or "instance_types" not in index:
        index["dates"]["last"] = f"{ref}"
        index["instance_types"] = {t: list(regions) for t, regions in rows.items()}
    index["generated_at"] = f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}"
    _put("index.json", index, 300)


def publish_day(day, agg, aggs):
    """하루 합계를 쓰고, 그날이 창에 들어가는 기준일(day+1 ~ day+7)의 점수와 index를 고친다.

    기준일 전날의 합계가 아직 없으면 그 기준일은 건너뛴다. 그날을 만들 때 다시 쓴다.
    완료 표시는 index까지 쓴 뒤 남긴다. 표시가 없는 날은 다음 실행이 다시 만든다.
    """
    aggs[day] = agg
    _put(f"agg/{day}.json", agg, 86400)
    latest = None
    for ref in (day + timedelta(k) for k in range(1, WINDOW + 1)):
        if ref - timedelta(WINDOW) < FIRST_DAY or load_agg(ref - timedelta(1), aggs) is None:
            continue
        window = [load_agg(ref - timedelta(k), aggs) for k in range(WINDOW, 0, -1)]
        rows = window_scores(window)
        _put(f"scores/{ref}.json", {"date": f"{ref}", "window": [f"{ref - timedelta(WINDOW)}",
                                    f"{ref - timedelta(1)}"], "columns": SCORE_COLUMNS, "rows": rows},
             86400)
        latest = (ref, rows)
    if latest:
        update_index(*latest)
    _put(f"agg/{day}.done", {}, 86400)


# ------------------------------------------------------------------ 진입점

def target_days(event, today):
    if "month" in event:
        m = date.fromisoformat(f"{event['month']}-01")
        days = [m + timedelta(d) for d in range(31) if (m + timedelta(d)).month == m.month]
    elif "date" in event:
        days = [date.fromisoformat(event["date"])]
    else:
        days = [today - timedelta(k) for k in range(LOOKBACK, 0, -1)]
        days = [d for d in days if not _exists(DST_BUCKET, f"{DST_PREFIX}/agg/{d}.done")]
    return [d for d in days if FIRST_DAY <= d < today]


def lambda_handler(event, context):
    today = datetime.now(timezone.utc).date()
    made, aggs = [], {}
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
                    # 기준일 day+1 ~ day+7의 창은 day-6 이후만 쓴다. 그 전 날은 메모리에서 뺀다.
                    for old in [d for d in aggs if d < day - timedelta(WINDOW - 1)]:
                        del aggs[old]
                    data = day_data(con, names, day)
                    agg = day_agg(data, day, summarize_day(data, day))
                    publish_day(day, agg, aggs)
                    made.append(f"{day}")
                    print(f"{day}: {len(agg['AZ'])} entries")
            finally:
                con.close()
    return {"days": made}
