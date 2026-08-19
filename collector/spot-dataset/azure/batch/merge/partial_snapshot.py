"""Build a usable Azure snapshot when SPS is unavailable."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


SPS_DEPENDENT_COLUMNS = (
    "DesiredCount",
    "AvailabilityZone",
    "Score",
    "T2",
    "T3",
)

OUTPUT_COLUMNS = (
    "InstanceTier",
    "InstanceType",
    "Region",
    "OndemandPrice",
    "SpotPrice",
    "Savings",
    "IF",
    "DesiredCount",
    "AvailabilityZone",
    "Score",
    "Time",
    "T2",
    "T3",
)

PRICE_IF_COLUMNS = (
    "InstanceTier",
    "InstanceType",
    "Region",
    "OndemandPrice",
    "SpotPrice",
    "Savings",
    "IF",
)


def _as_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def merge_price_and_if(
    price_df: pd.DataFrame | None,
    if_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """Merge the independent Azure Price and interruption-frequency results."""
    price_df = pd.DataFrame() if price_df is None else price_df.copy()
    if_df = pd.DataFrame() if if_df is None else if_df.copy()

    if not price_df.empty and not if_df.empty:
        price_df["_merge_type"] = price_df["InstanceType"].str.lower()
        price_df["_merge_tier"] = price_df["InstanceTier"].str.lower()
        if_df["_merge_type"] = if_df["InstanceType"].str.lower()
        if_df["_merge_tier"] = if_df["InstanceTier"].str.lower()

        merged = pd.merge(
            price_df,
            if_df,
            left_on=["_merge_type", "_merge_tier", "armRegionName"],
            right_on=["_merge_type", "_merge_tier", "Region"],
            how="outer",
        )
        merged["InstanceType"] = merged["InstanceType_x"].fillna(
            merged["InstanceType_y"]
        )
        merged["InstanceTier"] = merged["InstanceTier_x"].fillna(
            merged["InstanceTier_y"]
        )
        merged = merged[
            [
                "InstanceTier",
                "InstanceType",
                "Region_x",
                "OndemandPrice_x",
                "SpotPrice_x",
                "Savings_x",
                "IF",
            ]
        ]
        merged = merged[merged["SpotPrice_x"].notna()]
        return merged.rename(
            columns={
                "Region_x": "Region",
                "OndemandPrice_x": "OndemandPrice",
                "SpotPrice_x": "SpotPrice",
                "Savings_x": "Savings",
            }
        ).reset_index(drop=True)

    if not price_df.empty:
        price_df["IF"] = pd.NA
        return price_df[[column for column in PRICE_IF_COLUMNS if column in price_df]]

    if not if_df.empty:
        if_only = if_df.copy()
        for column in ("OndemandPrice", "SpotPrice", "Savings"):
            if_only[column] = pd.NA
        return if_only[[column for column in PRICE_IF_COLUMNS if column in if_only]]

    return pd.DataFrame(columns=PRICE_IF_COLUMNS)


def build_sps_unavailable_snapshot(
    price_saving_if_df: pd.DataFrame,
    timestamp: datetime,
) -> pd.DataFrame:
    """Keep collected Price/IF values and mark every SPS-derived value missing."""
    if price_saving_if_df is None or price_saving_if_df.empty:
        raise ValueError("Price or IF data is required for a partial snapshot")

    snapshot = price_saving_if_df.copy()

    for column in ("InstanceTier", "InstanceType", "Region"):
        if column not in snapshot.columns:
            snapshot[column] = pd.NA

    for column in ("OndemandPrice", "SpotPrice", "Savings", "IF"):
        if column not in snapshot.columns:
            snapshot[column] = pd.NA
        snapshot[column] = (
            pd.to_numeric(snapshot[column], errors="coerce")
            .replace(-1, pd.NA)
            .astype("Float64")
        )

    row_count = len(snapshot)
    snapshot["DesiredCount"] = pd.array([pd.NA] * row_count, dtype="Int64")
    snapshot["AvailabilityZone"] = pd.array(
        [pd.NA] * row_count, dtype="string"
    )
    snapshot["Score"] = pd.array([pd.NA] * row_count, dtype="Int64")
    snapshot["T2"] = pd.array([pd.NA] * row_count, dtype="Int64")
    snapshot["T3"] = pd.array([pd.NA] * row_count, dtype="Int64")
    snapshot["Time"] = _as_utc(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    snapshot = snapshot.loc[
        ~snapshot["Region"].astype(str).str.contains("gov", case=False, na=False)
    ]
    snapshot = snapshot[list(OUTPUT_COLUMNS)].drop_duplicates(
        subset=["InstanceTier", "InstanceType", "Region"]
    )

    if snapshot.empty:
        raise ValueError("Price or IF data produced no usable partial snapshot rows")

    return snapshot.reset_index(drop=True)


def build_partial_sps_snapshot(
    price_saving_if_df: pd.DataFrame,
    partial_sps_df: pd.DataFrame,
    timestamp: datetime,
) -> pd.DataFrame:
    """Keep completed SPS rows and mark only unfinished request ranges missing."""
    if price_saving_if_df is None or price_saving_if_df.empty:
        raise ValueError("Price or IF data is required for a partial SPS snapshot")
    if partial_sps_df is None or partial_sps_df.empty:
        raise ValueError("Partial SPS data is required for a partial SPS snapshot")

    join_keys = ["InstanceTier", "InstanceType", "Region"]
    missing_keys = [
        column
        for column in join_keys
        if column not in price_saving_if_df or column not in partial_sps_df
    ]
    if missing_keys:
        raise ValueError(f"Partial SPS data is missing join columns: {missing_keys}")

    sps = partial_sps_df.copy()
    if "time" in sps.columns:
        sps.rename(columns={"time": "Time"}, inplace=True)
    if "Time" not in sps.columns:
        sps["Time"] = pd.NA

    for column in ("DesiredCount", "AvailabilityZone", "Score", "T2", "T3"):
        if column not in sps.columns:
            sps[column] = pd.NA

    snapshot = pd.merge(
        price_saving_if_df.copy(),
        sps[
            join_keys
            + ["DesiredCount", "AvailabilityZone", "Score", "Time", "T2", "T3"]
        ],
        on=join_keys,
        how="inner",
    )
    if snapshot.empty:
        raise ValueError("Partial SPS data did not match any Price or IF rows")

    for column in ("OndemandPrice", "SpotPrice", "Savings", "IF"):
        if column not in snapshot.columns:
            snapshot[column] = pd.NA
        snapshot[column] = (
            pd.to_numeric(snapshot[column], errors="coerce")
            .replace(-1, pd.NA)
            .astype("Float64")
        )

    snapshot["DesiredCount"] = pd.to_numeric(
        snapshot["DesiredCount"], errors="coerce"
    ).astype("Int64")
    snapshot["AvailabilityZone"] = snapshot["AvailabilityZone"].astype("string")
    for column in ("Score", "T2", "T3"):
        snapshot[column] = pd.to_numeric(
            snapshot[column], errors="coerce"
        ).astype("Int64")

    snapshot["Time"] = snapshot["Time"].fillna(
        _as_utc(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    )
    snapshot = snapshot.loc[
        ~snapshot["Region"].astype(str).str.contains("gov", case=False, na=False)
    ]
    snapshot = snapshot[list(OUTPUT_COLUMNS)].drop_duplicates(
        subset=[
            "InstanceTier",
            "InstanceType",
            "Region",
            "DesiredCount",
            "AvailabilityZone",
        ]
    )

    if snapshot.empty:
        raise ValueError("Partial SPS data produced no usable snapshot rows")
    return snapshot.reset_index(drop=True)
