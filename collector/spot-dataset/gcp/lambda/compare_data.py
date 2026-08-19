import polars as pl

from utility import slack_msg_sender


def _annotate(df: pl.DataFrame, workload_cols: list[str], feature_cols: list[str]) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("Workload"),
                pl.lit(None, dtype=pl.Utf8).alias("Feature"),
            ]
        ).head(0)

    workload_expr = pl.concat_str(
        [pl.col(column).cast(pl.Utf8) for column in workload_cols],
        separator=":",
    ).alias("Workload")
    feature_expr = pl.concat_str(
        [pl.col(column).cast(pl.Utf8) for column in feature_cols],
        separator=":",
    ).alias("Feature")
    return df.with_columns([workload_expr, feature_expr]).sort("Workload")


def compare(
    previous_df: pl.DataFrame,
    current_df: pl.DataFrame,
    workload_cols: list[str],
    feature_cols: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    previous = _annotate(previous_df, workload_cols, feature_cols)
    current = _annotate(current_df, workload_cols, feature_cols)

    previous_map = {
        row["Workload"]: row["Feature"]
        for row in previous.select(["Workload", "Feature"]).iter_rows(named=True)
    }
    current_map = {
        row["Workload"]: row["Feature"]
        for row in current.select(["Workload", "Feature"]).iter_rows(named=True)
    }

    shared_workloads = previous_map.keys() & current_map.keys()
    changed_workloads = {
        workload
        for workload in current_map.keys()
        if workload not in previous_map or current_map[workload] != previous_map[workload]
    }
    removed_workloads = previous_map.keys() - current_map.keys()

    if any(workload not in previous_map and workload in shared_workloads for workload in changed_workloads):
        slack_msg_sender.send_slack_message("GCP compare workload invariant violated")
        raise Exception("workload error")

    changed_df = current.filter(pl.col("Workload").is_in(sorted(changed_workloads))).drop(
        ["Workload", "Feature"]
    )
    removed_df = previous.filter(pl.col("Workload").is_in(sorted(removed_workloads))).drop(
        ["Workload", "Feature"]
    )

    if not removed_df.is_empty():
        removed_df = removed_df.with_columns(
            [
                pl.lit(0).cast(removed_df.schema[column], strict=False).alias(column)
                for column in feature_cols
            ]
            + [pl.lit(True).alias("Ceased")]
        )
    else:
        removed_df = removed_df.with_columns(pl.lit(True).alias("Ceased"))

    return changed_df, removed_df
