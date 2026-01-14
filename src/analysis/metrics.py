import pandas as pd
import numpy as np


def calculate_execution_summary(
    gpe_events: pd.DataFrame, restpp_events: pd.DataFrame
) -> pd.DataFrame:
    """
    Correlates GPE Start/Stop times with RESTPP Return times to find
    queue delays and engine overhead.
    """
    if gpe_events.empty:
        return pd.DataFrame()

    gpe_bounds = gpe_events[gpe_events["event"].isin(["UDF_START", "UDF_STOP"])].copy()

    # Aggregation
    gpe_stats = (
        gpe_bounds.groupby(["run", "request_id"])
        .agg(
            start_udf_ts=("ts", "min"),
            stop_udf_ts=("ts", "max"),
            reported_udf_ms=("duration_ms", "max"),
        )
        .reset_index()
    )

    #  Coerce to datetime
    gpe_stats["start_udf_ts"] = pd.to_datetime(gpe_stats["start_udf_ts"], utc=True)
    gpe_stats["stop_udf_ts"] = pd.to_datetime(gpe_stats["stop_udf_ts"], utc=True)

    gpe_stats["actual_gpe_ms"] = (
        gpe_stats["stop_udf_ts"] - gpe_stats["start_udf_ts"]
    ).dt.total_seconds() * 1000.0

    #  Aggregations on RESTPP
    if restpp_events.empty:
        return _finalize_columns(gpe_stats)

    rest_stats = (
        restpp_events[restpp_events["event"] == "ReturnResult"]
        .groupby(["run", "request_id"])
        .agg(
            restpp_return_ts=("ts", "max"),
            restpp_duration_ms=("duration_ms", "max"),
            query_name=("query_name", "first"),
        )
        .reset_index()
    )

    #  Merge
    merged = pd.merge(gpe_stats, rest_stats, on=["run", "request_id"], how="left")

    #  Calculate Overheads
    if "restpp_return_ts" in merged.columns:
        merged["restpp_return_ts"] = pd.to_datetime(
            merged["restpp_return_ts"], utc=True
        )

        merged["post_process_overhead_ms"] = (
            merged["restpp_return_ts"] - merged["stop_udf_ts"]
        ).dt.total_seconds() * 1000.0
    else:
        merged["post_process_overhead_ms"] = np.nan

    return _finalize_columns(merged)


def _finalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Helper to ensure output always has the same shape, even if joins fail."""
    expected_cols = [
        "run",
        "request_id",
        "query_name",
        "actual_gpe_ms",
        "reported_udf_ms",
        "post_process_overhead_ms",
    ]
    # Add missing cols with NaN
    for c in expected_cols:
        if c not in df.columns:
            df[c] = np.nan

    return df
