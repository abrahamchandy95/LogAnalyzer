from typing import Final, cast

import pandas as pd

# identifies specific node execution
THREAD_KEY: Final[list[str]] = ["run", "node", "tid"]
# identifies a request instance
REQUEST_KEY: Final[list[str]] = ["run", "request_id"]
# identifies timeline events
TIMELINE_EVENTS: Final[list[str]] = ["UDF_START", "STEP", "UDF_STOP"]


def enrich_restpp_data(restpp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw RESTPP log events into a per-request metadata lookup
    """
    if restpp_df.empty:
        return pd.DataFrame()

    def _extract_name(series: pd.Series) -> str | None:
        return next((x for x in series if isinstance(x, str)), None)

    result = restpp_df.groupby(REQUEST_KEY, as_index=False).agg(
        query_name=("query_name", _extract_name),
        total_duration_ms=("duration_ms", "max"),
    )

    return cast(pd.DataFrame, result)


def repair_gpe_ids(gpe_events: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing request_ids in GPE logs
    """
    if gpe_events.empty:
        return gpe_events

    # sort for ffill
    df = gpe_events.sort_values(THREAD_KEY + ["ts"]).copy()
    # Assumes a thread processes one request at a time sequentially.
    df["request_id"] = df.groupby(THREAD_KEY)["request_id"].ffill()

    return df


def calculate_step_gaps(
    gpe_events: pd.DataFrame, restpp_enriched: pd.DataFrame
) -> pd.DataFrame:
    """
    Computes latency deltas between timeline events.
    """
    if gpe_events.empty:
        return pd.DataFrame()

    df = repair_gpe_ids(gpe_events)

    core = cast(
        pd.DataFrame,
        df[df["event"].isin(TIMELINE_EVENTS) & df["request_id"].notna()].copy(),
    )

    sort_cols = REQUEST_KEY + ["node", "tid", "ts", "lineno"]

    core = core.sort_values(by=sort_cols)

    group_cols = REQUEST_KEY + ["node", "tid"]
    grp = core.groupby(by=group_cols)

    core["prev_ts"] = grp["ts"].shift(1)

    core["gap_ms"] = (core["ts"] - core["prev_ts"]).dt.total_seconds() * 1000.0

    core["step_key"] = core["label"].astype(str).str.strip()

    gaps = cast(pd.DataFrame, core.dropna(subset=["gap_ms"]).copy())

    if not restpp_enriched.empty:
        gaps = gaps.merge(
            restpp_enriched[REQUEST_KEY + ["query_name"]], on=REQUEST_KEY, how="left"
        )

    return gaps
