import numpy as np
import pandas as pd

from common.utils import pct

_EMPTY_STEP_STATS_COLS = pd.Index(
    [
        "query_name",
        "step_key",
        "iteration",
        "n",
        "median_ms",
        "p95_ms",
        "mean_ms",
        "max_ms",
        "sum_ms",
    ]
)


def _median_ms(s: pd.Series) -> float:
    return pct(s, 50)


def _p95_ms(s: pd.Series) -> float:
    return pct(s, 95)


def make_step_stats(gaps_with_qname: pd.DataFrame) -> pd.DataFrame:
    if gaps_with_qname.empty:
        return pd.DataFrame(columns=_EMPTY_STEP_STATS_COLS)

    g = gaps_with_qname.dropna(subset=["query_name"]).copy()
    grp = g.groupby(["query_name", "step_key", "iteration"], dropna=False)

    # Avoid lambdas to satisfy basedpyright strict mode
    stats = grp.agg(
        n=("gap_ms", "count"),
        median_ms=("gap_ms", _median_ms),
        p95_ms=("gap_ms", _p95_ms),
        mean_ms=("gap_ms", "mean"),
        max_ms=("gap_ms", "max"),
        sum_ms=("gap_ms", "sum"),
    ).reset_index()

    # Typed-friendly stable two-pass sort
    stats = stats.sort_values(by="sum_ms", ascending=False, kind="mergesort")
    stats = stats.sort_values(by="query_name", ascending=True, kind="mergesort")
    return stats.reset_index(drop=True)


def compare_two_queries(
    stats: pd.DataFrame, base_name: str, opt_name: str
) -> pd.DataFrame:
    base = stats.loc[stats["query_name"] == base_name].copy()
    opt = stats.loc[stats["query_name"] == opt_name].copy()

    base = base.rename(
        columns={
            "n": "base_n",
            "median_ms": "base_median_ms",
            "p95_ms": "base_p95_ms",
            "mean_ms": "base_mean_ms",
            "max_ms": "base_max_ms",
            "sum_ms": "base_sum_ms",
        }
    ).drop(columns=["query_name"])

    opt = opt.rename(
        columns={
            "n": "opt_n",
            "median_ms": "opt_median_ms",
            "p95_ms": "opt_p95_ms",
            "mean_ms": "opt_mean_ms",
            "max_ms": "opt_max_ms",
            "sum_ms": "opt_sum_ms",
        }
    ).drop(columns=["query_name"])

    joined = base.merge(opt, on=["step_key", "iteration"], how="outer", indicator=True)

    # FUTURE-PROOF: `_merge` is Categorical; convert to string before replacing.
    merge_s = joined["_merge"].astype("string")
    joined["present_in"] = merge_s.replace(
        {"left_only": "base_only", "right_only": "opt_only", "both": "both"}
    )
    joined = joined.drop(columns=["_merge"])

    joined["opt_over_base_mean"] = joined["opt_mean_ms"] / joined["base_mean_ms"]
    joined["diff_mean_ms"] = joined["opt_mean_ms"] - joined["base_mean_ms"]

    joined["opt_over_base_median"] = joined["opt_median_ms"] / joined["base_median_ms"]
    joined["diff_median_ms"] = joined["opt_median_ms"] - joined["base_median_ms"]

    joined = joined.sort_values(
        by="opt_over_base_median", ascending=False, kind="mergesort"
    )
    joined = joined.sort_values(by="present_in", ascending=True, kind="mergesort")
    return joined.reset_index(drop=True)


def _pctl(s: pd.Series, p: float) -> float:
    x = s.dropna().to_numpy()
    return float(np.nanpercentile(x, p)) if len(x) else float("nan")


def _median_pctl(s: pd.Series) -> float:
    return _pctl(s, 50)


def _p95_pctl(s: pd.Series) -> float:
    return _pctl(s, 95)


def build_ordered_step_side_table(
    gapsq: pd.DataFrame,
    *,
    base_query: str,
    opt_query: str,
    step_prefix: str = "Step ",
) -> pd.DataFrame:
    if gapsq.empty:
        return pd.DataFrame()

    twoq_steps = gapsq.loc[
        gapsq["query_name"].isin([base_query, opt_query]) & (gapsq["event"] == "STEP")
    ].copy()
    if twoq_steps.empty:
        return pd.DataFrame()

    twoq_steps = twoq_steps.loc[
        twoq_steps["step_key"].astype(str).str.startswith(step_prefix)
    ].copy()
    if twoq_steps.empty:
        return pd.DataFrame()

    # Typed-friendly ordering
    twoq_steps = (
        twoq_steps.set_index(["query_name", "run", "request_id", "ts"])
        .sort_index()
        .reset_index()
    )
    twoq_steps["pos_in_request"] = (
        twoq_steps.groupby(["query_name", "run", "request_id"]).cumcount() + 1
    )

    pos_tbl = (
        twoq_steps.groupby(["query_name", "step_key"])["pos_in_request"]
        .median()
        .rename("median_pos")
        .reset_index()
    )

    step_stats = (
        twoq_steps.groupby(["query_name", "step_key"])
        .agg(
            n=("gap_ms", "size"),
            mean_ms=("gap_ms", "mean"),
            median_ms=("gap_ms", _median_pctl),
            p95_ms=("gap_ms", _p95_pctl),
            max_ms=("gap_ms", "max"),
            sum_ms=("gap_ms", "sum"),
        )
        .reset_index()
        .merge(pos_tbl, on=["query_name", "step_key"], how="left")
    )

    base = step_stats.loc[step_stats["query_name"] == base_query].copy()
    opt = step_stats.loc[step_stats["query_name"] == opt_query].copy()

    base = base.rename(
        columns={
            "n": "base_n",
            "mean_ms": "base_mean_ms",
            "median_ms": "base_median_ms",
            "p95_ms": "base_p95_ms",
            "max_ms": "base_max_ms",
            "sum_ms": "base_sum_ms",
            "median_pos": "base_pos",
        }
    ).drop(columns=["query_name"])

    opt = opt.rename(
        columns={
            "n": "opt_n",
            "mean_ms": "opt_mean_ms",
            "median_ms": "opt_median_ms",
            "p95_ms": "opt_p95_ms",
            "max_ms": "opt_max_ms",
            "sum_ms": "opt_sum_ms",
            "median_pos": "opt_pos",
        }
    ).drop(columns=["query_name"])

    side = base.merge(opt, on="step_key", how="outer", indicator=True)

    merge_s = side["_merge"].astype("string")
    side["present_in"] = merge_s.replace(
        {"left_only": "base_only", "right_only": "opt_only", "both": "both"}
    )
    side = side.drop(columns=["_merge"])

    side["pos"] = side[["base_pos", "opt_pos"]].median(axis=1, skipna=True)
    side = side.sort_values(["pos", "present_in"], na_position="last").reset_index(
        drop=True
    )

    side["select_like_idx"] = np.arange(1, len(side) + 1)
    side["diff_mean_ms"] = side["opt_mean_ms"] - side["base_mean_ms"]
    side["opt_over_base_mean"] = side["opt_mean_ms"] / side["base_mean_ms"]

    return side
