"""TITANS common utility functions."""
from __future__ import annotations

import pandas as pd


def filter_sentinel_rows(
    df: pd.DataFrame,
    value_columns: list[str],
    sentinel: int = -1,
) -> tuple[pd.DataFrame, int]:
    """Filter rows containing sentinel values (e.g., -1 from fillna).

    These rows represent missing data from outer joins, not real changes.
    Storing them would create false change points in TITANS.

    Returns:
        (filtered_df, dropped_count)
    """
    if df.empty:
        return df, 0
    cols = [c for c in value_columns if c in df.columns]
    if not cols:
        return df, 0
    mask = (df[cols] == sentinel).any(axis=1)
    return df[~mask].copy(), int(mask.sum())


def prepare_for_upload(
    changed_df: pd.DataFrame,
    removed_df: pd.DataFrame,
    value_columns: list[str] | None = None,
    pk_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Merge changed_df and removed_df with schema alignment.

    Args:
        changed_df: Changed records (Ceased=False)
        removed_df: Removed records (Ceased=True, set by compare_data.py)
        value_columns: If provided, filter rows with sentinel values (-1)
            from changed_df before merging.
        pk_columns: Primary key columns (e.g. ["InstanceType", "Region", "AZ"]).
            Used with "Time" to dedup Ceased duplicate rows.

    Returns:
        Combined DataFrame (schema aligned, sentinel rows filtered)
    """
    # Filter sentinel rows from changed_df (NOT from removed_df)
    if value_columns and not changed_df.empty:
        changed_df, dropped = filter_sentinel_rows(changed_df, value_columns)
        if dropped > 0:
            print(f"[TITANS] Filtered {dropped} rows with sentinel values (-1)")

    # Handle empty DataFrames
    if changed_df.empty and removed_df.empty:
        return pd.DataFrame()

    if changed_df.empty:
        return removed_df.copy()

    if removed_df.empty:
        # Add Ceased=False to changed_df
        result = changed_df.copy()
        if "Ceased" not in result.columns:
            result["Ceased"] = False
        return result

    # 1. Add Ceased=False to changed_df
    changed = changed_df.copy()
    if "Ceased" not in changed.columns:
        changed["Ceased"] = False

    # 2. removed_df already has Ceased=True (set by compare_data.py)
    removed = removed_df.copy()

    # 3. Align column order (ensure identical schema)
    all_cols = list(changed.columns)
    for col in removed.columns:
        if col not in all_cols:
            all_cols.append(col)

    # 4. Fill missing columns
    for col in all_cols:
        if col not in changed.columns:
            changed[col] = None
        if col not in removed.columns:
            removed[col] = None

    # 5. Concatenate
    combined_df = pd.concat(
        [changed[all_cols], removed[all_cols]],
        ignore_index=True
    )

    # 6. Dedup: collector may emit both Ceased=true and Ceased=false for
    #    the same PK+Time when a batch is re-processed (same time slot).
    #    Keep Ceased=false (real values) over Ceased=true (zeroed values).
    if "Ceased" in combined_df.columns and pk_columns:
        dedup_subset = pk_columns + ["Time"]
        dedup_subset = [c for c in dedup_subset if c in combined_df.columns]
        if dedup_subset:
            before = len(combined_df)
            combined_df = (
                combined_df
                .sort_values("Ceased", kind="stable")
                .drop_duplicates(subset=dedup_subset, keep="first")
                .reset_index(drop=True)
            )
            deduped = before - len(combined_df)
            if deduped > 0:
                print(f"[TITANS] Deduplicated {deduped} ceased duplicate rows (same PK+Time)")

    return combined_df
