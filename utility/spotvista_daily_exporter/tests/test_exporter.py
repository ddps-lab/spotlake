import math
import random
import sys
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lambda_function as exporter

DAY = date(2026, 10, 11)
D0 = int(datetime.combine(DAY, time(), timezone.utc).timestamp()) // 60


def make_data(rows, n_pk):
    """rows: (항목 번호, 분, T3, SPS, SpotPrice). load_month 결과와 같은 모양으로 만든다."""
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


if __name__ == "__main__":
    unittest.main()
