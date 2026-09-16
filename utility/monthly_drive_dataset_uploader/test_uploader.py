"""AWS 없이 도는 검사. python3 test_uploader.py

여기서 보는 것은 달 계산과 스키마 정규화 두 가지다. 나머지는 S3와 Drive를
불러야 해서 실제 실행으로 확인한다.
"""

from datetime import date, datetime
import gzip
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

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
    assert out["Brandnew"].dtype == pl.Int64


def test_build_day_with_changing_csv_numeric_schemas():
    # 실제 실패 두 가지: T2/T3 없음 -> 정수, IF 소수 -> 정수(-1).
    prefix = "rawdata/azure/2025/12/12/"
    header = "InstanceTier,InstanceType,Region,AvailabilityZone,IF,Score"
    payloads = {
        prefix + "00-00-00.csv.gz": gzip.compress(
            (header + "\nStandard,D2s_v3,us,zone1,1.5,Low\n").encode()),
        prefix + "12-00-00.csv.gz": gzip.compress(
            (header + ",T2,T3\nStandard,D2s_v3,us,zone1,-1,3,0,25\n").encode()),
    }
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "day.parquet"
        with patch.object(m, "raw_keys", return_value=list(payloads)), \
                patch.object(m, "fetch", side_effect=lambda key: (key, payloads[key])):
            assert m.build_day("azure", 2025, 12, 12, path) == (2, 2)
        out = pl.read_parquet(path)
    assert all(out[c].dtype == dtype for c, dtype in m.VALUE_DTYPES["azure"].items())
    assert out["Score"].to_list() == ["Low", "3"]
    assert out["IF"].to_list() == [1.5, -1.0]
    assert out["T2"].to_list() == [None, 0]
    assert out["T3"].to_list() == [None, 25]
    assert out["AZ"].to_list() == ["zone1", "zone1"]
    assert out["Time"].to_list() == [datetime(2025, 12, 12), datetime(2025, 12, 12, 12)]


def test_normalize_rejects_invalid_numeric_values():
    df = pl.DataFrame({"InstanceType": ["n1"], "Region": ["us"],
                       "SpotPrice": ["invalid"]})
    try:
        m.normalize(df, "gcp", datetime(2025, 3, 16))
    except pl.exceptions.InvalidOperationError:
        pass
    else:
        raise AssertionError("Invalid numeric data must not silently become null")


def test_integer_fields_preserve_null_and_reject_fractional_values():
    df = pl.DataFrame({"SPS": [3.0, None, -1.0], "T3": [10, None, 0]})
    out = m.cast_integer_columns(df, "aws")
    assert out["SPS"].dtype == pl.Int64
    assert out["SPS"].to_list() == [3, None, -1]
    assert out["T3"].to_list() == [10, None, 0]
    for invalid in [1.5, float("nan"), float("inf"), float("-inf")]:
        try:
            m.cast_integer_columns(pl.DataFrame({"T2": [invalid]}), "azure")
        except ValueError:
            pass
        else:
            raise AssertionError(f"Must reject fractional/nonfinite count: {invalid}")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("전부 통과")
