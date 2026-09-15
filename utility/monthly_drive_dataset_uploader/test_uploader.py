"""AWS 없이 도는 검사. python3 test_uploader.py

여기서 보는 것은 달 계산과 스키마 정규화 두 가지다. 나머지는 S3와 Drive를
불러야 해서 실제 실행으로 확인한다.
"""

from datetime import date, datetime

import polars as pl

import monthly_drive_dataset_uploader as m


def test_month_math():
    assert m.add_months(2026, 1, -1) == (2025, 12)
    assert m.add_months(2025, 12, 1) == (2026, 1)
    assert m.add_months(2026, 9, -18) == (2025, 3)

    w = m.window_months(date(2026, 9, 14))
    assert w[0] == (2025, 3) and w[-1] == (2026, 3)
    assert len(w) == 13, len(w)
    # 창은 이어져 있어야 한다. 한 달이라도 비면 그 달은 영영 안 올라간다.
    assert all(m.add_months(*a, 1) == b for a, b in zip(w, w[1:]))

    assert m.label(2025, 3) == "2025-03"


def test_tick_of():
    key = "rawdata/azure/2025/03/16/23-45-01.csv.gz"
    assert m.tick_of(key, 2025, 3, 16) == datetime(2025, 3, 16, 23, 45, 1)


def test_normalize_renames_and_fills():
    # 2025년 azure 모양. AvailabilityZone 이름이 다르고 T2/T3 가 없다.
    df = pl.DataFrame({
        "id": [1],
        "InstanceTier": ["Standard"],
        "InstanceType": ["D2s_v3"],
        "Region": ["koreacentral"],
        "AvailabilityZone": ["1"],
        "Score": [3],
        "OndemandPrice": [0.1],
        "SpotPrice": [0.03],
        "Savings": [70],
        "DesiredCount": [1],
        "IF": [0.05],
    })
    out = m.normalize(df, "azure", datetime(2025, 3, 16))

    assert out.columns == ["Time"] + m.PK_COLUMNS["azure"] + m.VALUE_ORDER["azure"]
    assert "id" not in out.columns          # 내부용 컬럼은 빠진다
    assert out["AZ"][0] == "1"              # 이름이 바뀐다
    assert out["T2"][0] is None             # 없던 컬럼은 null 로 채운다
    assert out["Time"][0] == datetime(2025, 3, 16)


def test_normalize_keeps_unknown_columns():
    # 나중에 컬럼이 늘어도 버리지 않고 뒤에 붙인다.
    df = pl.DataFrame({"InstanceType": ["n1"], "Region": ["us"], "Brandnew": [1]})
    out = m.normalize(df, "gcp", datetime(2025, 3, 16))
    assert out.columns[-1] == "Brandnew"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("전부 통과")
