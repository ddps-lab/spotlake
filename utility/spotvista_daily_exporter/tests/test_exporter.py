import json
import math
import random
import shutil
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lambda_function as exporter

DAY = date(2026, 10, 11)
D0 = int(datetime.combine(DAY, time(), timezone.utc).timestamp()) // 60


def make_data(rows, n_pk):
    """rows: (항목 번호, 분, T3, SPS, SpotPrice). day_data 결과와 같은 모양으로 만든다."""
    rows = sorted(rows, key=lambda r: (r[0], r[1]))
    col = lambda i: np.array([r[i] for r in rows], dtype=float)
    return {"names": [("t", "r", f"az{k}") for k in range(n_pk)],
            "code": np.array([r[0] for r in rows], dtype=np.int64),
            "minute": np.array([r[1] for r in rows], dtype=np.int64),
            "T3": col(2), "SPS": col(3), "SpotPrice": col(4)}


def brute(rows, k):
    """칸마다 그 칸이 끝나기 전의 마지막 행을 쓰는 직접 계산."""
    mine = sorted((r for r in rows if r[0] == k), key=lambda r: r[1])
    grid = []
    for tick in range(exporter.TICKS):
        seen = [r for r in mine if r[1] < D0 + 10 * (tick + 1)]
        grid.append(seen[-1] if seen else None)
    y = [g[2] if g and not math.isnan(g[2]) else None for g in grid]
    v = [x for x in y if x is not None]
    paid = [g[4] for g, x in zip(grid, y) if x is not None and g[4] > 0]
    return {"n": len(v), "sum": sum(v), "sumsq": sum(x * x for x in v),
            "sum_iy": sum(i * x for i, x in enumerate(v)),
            "first": v[0] if v else None, "last": v[-1] if v else None,
            "SpotPrice": paid[-1] if paid else None}


class SummarizeDayTest(unittest.TestCase):
    def check(self, rows, n_pk):
        s = exporter.summarize_day(make_data(rows, n_pk), DAY)
        for k in range(n_pk):
            want = brute(rows, k)
            self.assertEqual(int(s["n"][k]), want["n"], k)
            if not want["n"]:
                continue
            for f in ("sum", "sumsq", "sum_iy", "first", "last"):
                self.assertEqual(s[f][k], want[f], (k, f))
            got = s["SpotPrice"][k]
            self.assertEqual(None if np.isnan(got) else got, want["SpotPrice"], k)

    def test_carry_in_change_and_next_day(self):
        nan = math.nan
        rows = [
            (0, D0 - 500, 50, 3, 0.1, ), (0, D0 + 300, 45, 3, 0.1),   # 전날 값이 이어지다 05:00에 바뀜
            (0, D0 + 1440, 5, 3, 0.1),                                  # 다음 날 00:00 행은 빠진다
            (1, D0 + 600, 20, 3, 0.2),                                  # 10:00에 처음 나타남
            (2, D0 - 10, 30, 3, 0.3), (2, D0 + 100, nan, nan, nan),     # Ceased 뒤 다시 나타남
            (2, D0 + 200, 35, 3, -1.0),                                 # 가격을 못 받은 행
            (3, D0, 10, 1, 0.4), (3, D0 + 3, 15, 1, 0.4), (3, D0 + 7, 25, 1, 0.4),  # 한 칸에 여러 행
        ]
        self.check(rows, 4)
        s = exporter.summarize_day(make_data(rows, 4), DAY)
        self.assertEqual(int(s["n"][1]), 84)
        self.assertEqual(int(s["n"][2]), 134)
        self.assertEqual(s["SpotPrice"][2], 0.3)
        self.assertEqual(s["first"][3], 25)

    def test_random_against_brute_force(self):
        rnd = random.Random(7)
        levels = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
        rows = []
        for k in range(30):
            for _ in range(rnd.randint(0, 25)):
                minute = D0 + rnd.choice([-2000, -10, 0]) if rnd.random() < 0.2 else D0 + rnd.randint(1, 1500)
                if rnd.random() < 0.1:
                    rows.append((k, minute, math.nan, math.nan, math.nan))
                else:
                    rows.append((k, minute, rnd.choice(levels), 3, rnd.choice([0.1, 0.2, -1.0])))
        rows = list({(r[0], r[1]): r for r in rows}.values())
        self.check(rows, 30)


def write_parquet(path, columns, values):
    import duckdb
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM (VALUES {values}) t({columns})) "
                f"TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()


def load_day(files, day):
    """parquet 파일들(앞이 월초 상태)을 open_month로 올려 day_data를 꺼낸다."""
    until = datetime.combine(day, time(23, 50), timezone.utc)
    keys = [str(i) for i in range(len(files))]
    with tempfile.TemporaryDirectory() as tmp:
        s3 = MagicMock()
        s3.download_file.side_effect = lambda bucket, key, dest: shutil.copy(files[int(key)], dest)
        with patch.object(exporter, "month_files", return_value=keys), \
                patch.object(exporter, "_s3", return_value=s3):
            con, names = exporter.open_month(day.replace(day=1), until, tmp)
            try:
                return exporter.day_data(con, names, day)
            finally:
                con.close()


class LoadMonthTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def path(self, name):
        return f"{self.dir}/{name}.parquet".replace("\\", "/")

    def test_day_rows_from_zstd_files_in_utc(self):
        # 앞은 T3·Ceased가 없던 시기 형식, 뒤는 지금 형식이다.
        old, new = self.path("old"), self.path("new")
        write_parquet(old, "InstanceType, Region, AZ, Time, SPS, SpotPrice, IF", """
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-09-30 23:00:00+00', 3, 0.1, 1.0),
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-10-01 00:00:00+00', 2, 0.2, 1.0)""")
        write_parquet(new, "InstanceType, Region, AZ, Time, SPS, T3, SpotPrice, Ceased", """
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-01 00:10:00+00', 3, 50, 0.1, false),
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-01 00:20:00+00', 0, 0, 0.0, true),
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-10-01 00:00:00+00', 2, 5, 0.2, false),
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-02 00:00:00+00', 3, 45, 0.1, false)""")
        data = load_day([old, new], date(2026, 10, 1))

        minute = lambda *t: int(datetime(*t, tzinfo=timezone.utc).timestamp()) // 60
        self.assertEqual(data["names"], [("a", "r", "z1"), ("b", "r", "z2")])
        # a: 00:00 이전 마지막 행과 그날 두 행(until 이후 행은 빠짐), b: 00:00 행
        self.assertEqual(data["code"].tolist(), [0, 0, 0, 1])
        # 이 테스트를 KST에서 돌려도 UTC 기준 분이어야 한다.
        self.assertEqual(data["minute"].tolist(), [minute(2026, 9, 30, 23), minute(2026, 10, 1, 0, 10),
                                                   minute(2026, 10, 1, 0, 20), minute(2026, 10, 1)])
        t3 = data["T3"].tolist()
        self.assertTrue(math.isnan(t3[0]))                       # T3 컬럼이 없던 파일
        self.assertEqual(t3[1], 50)
        self.assertTrue(math.isnan(t3[2]))                       # Ceased 행
        self.assertEqual(t3[3], 5)                               # 두 파일에 있으면 뒤 파일
        self.assertEqual(data["SPS"].tolist()[0], 3)

    def test_normal_row_wins_over_ceased_at_same_time(self):
        # 같은 항목·시각에 정상 행과 Ceased 행이 함께 있으면 warm_compactor처럼 정상 행을 쓴다.
        # a는 한 파일 안의 중복(리뷰 재현), b는 00:00 이전 carry-in과 뒤 파일의 Ceased 행이다.
        cols = "InstanceType, Region, AZ, Time, SPS, T3, SpotPrice, Ceased"
        first, second = self.path("first"), self.path("second")
        write_parquet(first, cols, """
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-01 00:00:00+00', 3, 50, 0.1, false),
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-01 12:00:00+00', 3, 40, 0.1, false),
            ('a', 'r', 'z1', TIMESTAMPTZ '2026-10-01 12:00:00+00', 0, 0, 0.0, true),
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-09-30 23:00:00+00', 0, 0, 0.0, true),
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-09-30 23:00:00+00', 3, 30, 0.1, false),
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-10-01 12:00:00+00', 3, 40, 0.1, false)""")
        write_parquet(second, cols, """
            ('b', 'r', 'z2', TIMESTAMPTZ '2026-10-01 12:00:00+00', 0, 0, 0.0, true)""")
        day = date(2026, 10, 1)
        s = exporter.summarize_day(load_day([first, second], day), day)
        self.assertEqual((int(s["n"][0]), s["sum"][0]), (144, 6480))
        self.assertEqual((int(s["n"][1]), s["sum"][1]), (144, 72 * 30 + 72 * 40))


def original_score(y):
    """SpotVista-Query calculate_score.py의 calculate_score와 get_slope (pandas Series 대신 배열)."""
    y = np.asarray(y, dtype=float)
    n = len(y)
    x = np.linspace(0, n - 1, n)
    max_slope = np.polyfit(x, np.linspace(0, 50, n).astype(int), 1)[0]
    actual = np.polyfit(x, y, 1)[0]
    slope = (actual / -max_slope) * -0.1 if actual < 0 else actual / max_slope * 0.1
    integral = np.trapezoid(y) / np.trapezoid([50] * n)
    return (integral - integral * (y.std(ddof=1) / 25 * 0.1) + integral * slope) * 100


def agg_of(day, entries):
    """entries: {(type, region, az): (하루 T3 값 목록(None은 없음), SPS, SpotPrice)} -> day_agg 모양."""
    out = {"date": f"{day}", **{f: [] for f in ("InstanceType", "Region", "AZ", "SPS", "SpotPrice")
                                 + exporter.AGG_FIELDS}}
    for (t, r, az), (values, sps, price) in entries.items():
        v = [x for x in values if x is not None]
        if not v:
            continue
        for f, x in zip(("InstanceType", "Region", "AZ", "SPS", "SpotPrice"), (t, r, az, sps, price)):
            out[f].append(x)
        for f, x in zip(exporter.AGG_FIELDS, (len(v), sum(v), sum(a * a for a in v),
                                              sum(i * a for i, a in enumerate(v)), v[0], v[-1])):
            out[f].append(x)
    return out


class WindowScoresTest(unittest.TestCase):
    def test_matches_original_calculate_score(self):
        rnd = random.Random(3)
        keys = [(t, r, f"{r}-az{k}") for t in ("m5.large", "c6i.xlarge") for r in ("us-east-1", "ap-northeast-2")
                for k in range(10)]
        days = [DAY + timedelta(d) for d in range(exporter.WINDOW)]
        raw = {k: [] for k in keys}
        aggs = []
        for d in days:
            entries = {}
            for k in keys:
                if rnd.random() < 0.15:          # 그날 없는 항목
                    continue
                level = rnd.choice([0, 5, 20, 35, 50])
                values = []
                for _ in range(exporter.TICKS):
                    if rnd.random() < 0.05:
                        level = rnd.choice([0, 5, 10, 20, 30, 40, 45, 50])
                    values.append(None if rnd.random() < 0.03 else level)
                entries[k] = (values, rnd.choice([3, 2, 0, None]), rnd.choice([0.1, 0.25, 0.0, None]))
                raw[k].append((d, entries[k]))
            aggs.append(agg_of(d, entries))
        aggs[2] = None                            # 하루 통째로 없는 날

        rows = exporter.window_scores(aggs)
        got = {(t, r, row[0]): row for t, regions in rows.items() for r, rs in regions.items() for row in rs}
        present = {k: [e for d, e in raw[k] if d != days[2]] for k in keys}
        checked = 0
        for k in keys:
            entries = [e for e in present[k] if any(x is not None for x in e[0])]
            y = [x for e in entries for x in e[0] if x is not None]
            last_values, sps, price = entries[-1] if entries else (None, None, None)
            if len(y) < 2 or not sps or not price:
                self.assertNotIn(k, got)
                continue
            az, got_sps, got_t3, got_price, got_score = got[k]
            self.assertEqual((got_sps, got_t3, got_price), (sps, y[-1], price), k)
            self.assertAlmostEqual(got_score, original_score(y), delta=6e-5, msg=k)
            checked += 1
        self.assertGreater(checked, 5)


class PublishDayTest(unittest.TestCase):
    """완료 표시는 index까지 쓴 뒤에 남는다. index에서 실패하면 다음 실행이 그날을 다시 만든다."""

    def run_day(self, fail_index):
        store = {}

        def put(key, body, max_age):
            if fail_index and key == "index.json":
                raise RuntimeError("index upload failed")
            store[key] = body

        exists = lambda bucket, key: key.removeprefix(f"{exporter.DST_PREFIX}/") in store
        agg = agg_of(DAY, {("m5.large", "us-east-1", "use1-az1"): ([50] * exporter.TICKS, 3, 0.1)})
        with patch.object(exporter, "_put", side_effect=put), patch.object(exporter, "_exists", side_effect=exists):
            try:
                exporter.publish_day(DAY, agg, {})
            except RuntimeError:
                pass
            return store, exporter.target_days({}, DAY + timedelta(1))

    def test_index_failure_leaves_day_to_retry(self):
        store, retry = self.run_day(fail_index=True)
        self.assertIn(f"scores/{DAY + timedelta(1)}.json", store)
        self.assertNotIn(f"agg/{DAY}.done", store)
        self.assertIn(DAY, retry)

    def test_success_marks_day_done(self):
        store, retry = self.run_day(fail_index=False)
        scores = store[f"scores/{DAY + timedelta(1)}.json"]
        self.assertEqual(scores["window"], [f"{DAY - timedelta(6)}", f"{DAY}"])
        self.assertEqual(scores["rows"]["m5.large"]["us-east-1"][0][:4], ["use1-az1", 3, 50, 0.1])
        self.assertEqual(store["index.json"]["instance_types"], {"m5.large": ["us-east-1"]})
        self.assertNotIn(DAY, retry)
        json.dumps(store)                         # 모두 JSON으로 쓸 수 있어야 한다


if __name__ == "__main__":
    unittest.main()
