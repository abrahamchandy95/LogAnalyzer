import pandas as pd


_EMPTY_GAPS_COLS = pd.Index(
    [
        "run",
        "node",
        "tid",
        "request_id",
        "ts",
        "event",
        "label",
        "iteration",
        "udf",
        "detail",
        "gap_ms",
        "prev_ts",
        "prev_event",
        "prev_label",
        "log_path",
        "lineno",
        "step_key",
    ]
)


def _get_series(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return df[col] as a Series if it exists and is a Series; otherwise None."""
    v = df.get(col)
    return v if isinstance(v, pd.Series) else None


def _as_df(x: object) -> pd.DataFrame | None:
    """Narrow unknown pandas return types to DataFrame."""
    return x if isinstance(x, pd.DataFrame) else None


def build_gaps(gpe_events: pd.DataFrame) -> pd.DataFrame:
    """
    Compute gap_ms between consecutive UDF boundary logs within (run,node,tid,request_id).
    """
    if gpe_events.empty:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    event_s = _get_series(gpe_events, "event")
    if event_s is None:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    mask_events = event_s.isin(("UDF_START", "STEP", "UDF_STOP"))
    core0 = _as_df(gpe_events.loc[mask_events].copy())
    if core0 is None or core0.empty:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    request_id_s = _get_series(core0, "request_id")
    if request_id_s is None:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    # Avoid Series.notna() (pyright sometimes thinks request_id_s is ndarray); use pd.notna
    core1 = _as_df(core0.loc[pd.notna(request_id_s)].copy())
    if core1 is None or core1.empty:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    # Typed-friendly multi-key ordering without sort_values(list[str])
    core2 = _as_df(
        core1.set_index(["run", "node", "request_id", "tid", "ts"])
        .sort_index()
        .reset_index()
    )
    if core2 is None or core2.empty:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    grp_cols = ["run", "node", "request_id", "tid"]

    core2["prev_ts"] = core2.groupby(grp_cols)["ts"].shift(1)
    core2["prev_event"] = core2.groupby(grp_cols)["event"].shift(1)
    core2["prev_label"] = core2.groupby(grp_cols)["label"].shift(1)

    ts_s = _get_series(core2, "ts")
    prev_ts_s = _get_series(core2, "prev_ts")
    if ts_s is None or prev_ts_s is None:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    core2["gap_ms"] = (ts_s - prev_ts_s).dt.total_seconds() * 1000.0

    mask_prev = pd.notna(prev_ts_s)
    out0 = _as_df(core2.loc[mask_prev].copy())
    if out0 is None or out0.empty:
        return pd.DataFrame(columns=_EMPTY_GAPS_COLS)

    label_s = _get_series(out0, "label")
    if label_s is None:
        out0["step_key"] = pd.Series(dtype="string")
        return out0

    labels = label_s.astype("string")
    out0["step_key"] = labels.str.replace(r"\s+", " ", regex=True).str.strip()
    return out0


def add_query_name(gaps: pd.DataFrame, restpp_req: pd.DataFrame) -> pd.DataFrame:
    if gaps.empty:
        out = gaps.copy()
        out["query_name"] = pd.Series(dtype="object")
        return out

    req = restpp_req[
        ["run", "request_id", "query_name", "endpoint", "restpp_return_ms"]
    ].copy()
    merged0 = gaps.merge(req, on=["run", "request_id"], how="left")

    merged = _as_df(merged0)
    if merged is None:
        # Defensive fallback for stubbed union types (should not happen at runtime)
        return pd.DataFrame(merged0)

    return merged
