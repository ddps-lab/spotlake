"""TITANS common utility functions."""
import pandas as pd


def prepare_for_upload(changed_df: pd.DataFrame, removed_df: pd.DataFrame) -> pd.DataFrame:
    """Merge changed_df and removed_df with schema alignment.

    Args:
        changed_df: Changed records (Ceased=False)
        removed_df: Removed records (Ceased=True, set by compare_data.py)

    Returns:
        Combined DataFrame (schema aligned)
    """
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

    return combined_df
