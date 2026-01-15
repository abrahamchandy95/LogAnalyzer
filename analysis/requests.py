import pandas as pd


def _as_df(x: object) -> pd.DataFrame:
    """Narrow pandas-return unions to DataFrame for type checkers."""
    if isinstance(x, pd.DataFrame):
        return x
    return pd.DataFrame(x)


def summarize_requests(
    restpp_req: pd.DataFrame, gpe_attached: pd.DataFrame
) -> pd.DataFrame:
    """
    Per-request view:
      - first/last seen in GPE
      - udf start/stop timestamps
      - reported Stop_RunUDF ms vs actual wall-clock between start/stop
      - RESTPP first seen + ReturnResult timestamp/ms
    """
    # Avoid dropna(subset=...) stub issues by masking
    rid_s = gpe_attached.get("request_id")
    if isinstance(rid_s, pd.Series):
        g = _as_df(gpe_attached.loc[pd.notna(rid_s)].copy())
    else:
        g = _as_df(gpe_attached.copy())

    if g.empty:
        gpe_sum = pd.DataFrame(
            columns=pd.Index(
                [
                    "run",
                    "request_id",
                    "gpe_node",
                    "first_seen_gpe_ts",
                    "last_seen_gpe_ts",
                    "start_udf_ts",
                    "stop_udf_ts",
                    "reported_stop_udf_ms",
                    "actual_stop_udf_ms",
                    "actual_diff_first_last_seen_ms",
                    "diff_gpe_duration_udf_ms",
                ]
            )
        )
    else:

        def _first_node(s: pd.Series) -> str | None:
            return next((x for x in s if isinstance(x, str)), None)

        grp = g.groupby(["run", "request_id"], as_index=False)
        gpe_sum = _as_df(
            grp.agg(
                gpe_node=("node", _first_node),
                first_seen_gpe_ts=("ts", "min"),
                last_seen_gpe_ts=("ts", "max"),
            )
        )

        starts = (
            g.loc[g["event"] == "UDF_START"]
            .groupby(["run", "request_id"])["ts"]
            .min()
            .rename("start_udf_ts")
        )
        stops = (
            g.loc[g["event"] == "UDF_STOP"]
            .groupby(["run", "request_id"])["ts"]
            .max()
            .rename("stop_udf_ts")
        )
        reported = (
            g.loc[g["event"] == "UDF_STOP"]
            .groupby(["run", "request_id"])["udf_ms"]
            .max()
            .rename("reported_stop_udf_ms")
        )

        gpe_sum = _as_df(gpe_sum.merge(starts, on=["run", "request_id"], how="left"))
        gpe_sum = _as_df(gpe_sum.merge(stops, on=["run", "request_id"], how="left"))
        gpe_sum = _as_df(gpe_sum.merge(reported, on=["run", "request_id"], how="left"))

        gpe_sum["actual_stop_udf_ms"] = (
            gpe_sum["stop_udf_ts"] - gpe_sum["start_udf_ts"]
        ).dt.total_seconds() * 1000.0
        gpe_sum["actual_diff_first_last_seen_ms"] = (
            gpe_sum["last_seen_gpe_ts"] - gpe_sum["first_seen_gpe_ts"]
        ).dt.total_seconds() * 1000.0
        gpe_sum["diff_gpe_duration_udf_ms"] = (
            gpe_sum["actual_diff_first_last_seen_ms"] - gpe_sum["actual_stop_udf_ms"]
        )

    r = restpp_req.copy()
    if "restpp_return_ts" not in r.columns:
        r["restpp_return_ts"] = pd.NaT

    cols = [
        "run",
        "request_id",
        "restpp_node",
        "restpp_ts",
        "restpp_return_ts",
        "restpp_return_ms",
        "graph_name",
        "query_name",
        "endpoint",
    ]
    rsel = _as_df(r.loc[:, cols].copy())

    # IMPORTANT: rename(columns=...) on a guaranteed DataFrame
    rsum = rsel.rename(
        columns={
            "restpp_ts": "first_seen_ts_restpp",
            "restpp_return_ts": "sync_return_result_ts_restpp",
            "restpp_return_ms": "sync_return_time_ms",
            "endpoint": "full_endpoint",
        }
    )

    out = _as_df(gpe_sum.merge(rsum, on=["run", "request_id"], how="left"))

    fe = out.get("full_endpoint")
    if isinstance(fe, pd.Series):
        out["endpoint_name"] = fe.astype("string").str.extract(
            r"/query/[^/]+/(?P<q>[^?\s|]+)", expand=False
        )
    else:
        out["endpoint_name"] = pd.Series(dtype="string")

    out = out.set_index(["run", "first_seen_gpe_ts"]).sort_index().reset_index()
    return out.reset_index(drop=True)


def build_exec_request_table(
    restpp_req: pd.DataFrame, gpe_attached: pd.DataFrame
) -> pd.DataFrame:
    """
    One row per (run, request_id) that has BOTH UDF_START and UDF_STOP in GPE,
    with actual_stop_udf_ms computed from timestamps and query_name joined from RESTPP.
    """
    rid_s = gpe_attached.get("request_id")
    if isinstance(rid_s, pd.Series):
        g = _as_df(gpe_attached.loc[pd.notna(rid_s)].copy())
    else:
        g = _as_df(gpe_attached.copy())

    if g.empty:
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "run",
                    "request_id",
                    "start_udf_ts",
                    "stop_udf_ts",
                    "actual_stop_udf_ms",
                    "reported_stop_udf_ms",
                    "query_name",
                    "graph_name",
                    "endpoint",
                ]
            )
        )

    starts = (
        g.loc[g["event"] == "UDF_START"]
        .groupby(["run", "request_id"])["ts"]
        .min()
        .rename("start_udf_ts")
    )
    stops = (
        g.loc[g["event"] == "UDF_STOP"]
        .groupby(["run", "request_id"])["ts"]
        .max()
        .rename("stop_udf_ts")
    )
    reported = (
        g.loc[g["event"] == "UDF_STOP"]
        .groupby(["run", "request_id"])["udf_ms"]
        .max()
        .rename("reported_stop_udf_ms")
    )

    exec_tbl = _as_df(pd.concat([starts, stops, reported], axis=1).reset_index())

    exec_tbl = exec_tbl.dropna(subset=["start_udf_ts", "stop_udf_ts"]).copy()
    exec_tbl["actual_stop_udf_ms"] = (
        exec_tbl["stop_udf_ts"] - exec_tbl["start_udf_ts"]
    ).dt.total_seconds() * 1000.0

    rcols = [
        c
        for c in ["run", "request_id", "query_name", "graph_name", "endpoint"]
        if c in restpp_req.columns
    ]
    rsel = _as_df(restpp_req.loc[:, rcols].copy())

    # IMPORTANT: use subset=... (not positional list) and do it on a guaranteed DataFrame
    rmap = rsel.drop_duplicates(subset=["run", "request_id"])

    exec_tbl = _as_df(exec_tbl.merge(rmap, on=["run", "request_id"], how="left"))

    exec_tbl = exec_tbl.set_index(["run", "start_udf_ts"]).sort_index().reset_index()
    return exec_tbl.reset_index(drop=True)


def extract_ids(
    exec_tbl: pd.DataFrame, base_q: str, opt_q: str
) -> tuple[list[str], list[str]]:
    base_ids = (
        exec_tbl.loc[exec_tbl["query_name"] == base_q, "request_id"]
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()
    )
    opt_ids = (
        exec_tbl.loc[exec_tbl["query_name"] == opt_q, "request_id"]
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()
    )
    return base_ids, opt_ids
