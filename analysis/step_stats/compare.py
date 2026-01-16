import numpy as np
import pandas as pd

from analysis import dfkeys as K


def _prefixed_map(prefix: str, cols: list[str]) -> dict[str, str]:
    return {c: f"{prefix}{c}" for c in cols}


def _add_present_in(df: pd.DataFrame) -> pd.DataFrame:
    merge_s = df["_merge"].astype("string")
    out = df.copy()
    out[K.PRESENT_IN] = merge_s.replace(
        {"left_only": "base_only", "right_only": "opt_only", "both": "both"}
    )
    return out.drop(columns=["_merge"])


def compare_two_queries(
    stats: pd.DataFrame, base_name: str, opt_name: str
) -> pd.DataFrame:
    """
    Join per-step stats for base + opt queries and compute deltas/ratios.
    Input must look like output of make_step_stats().
    """
    if stats.empty:
        return pd.DataFrame()

    base = stats.loc[stats[K.QUERY_NAME] == base_name].copy()
    opt = stats.loc[stats[K.QUERY_NAME] == opt_name].copy()

    # Columns to prefix
    metric_cols = [K.N, K.MEDIAN_MS, K.P95_MS, K.MEAN_MS, K.MAX_MS, K.SUM_MS]

    base = base.rename(columns=_prefixed_map("base_", metric_cols)).drop(
        columns=[K.QUERY_NAME]
    )
    opt = opt.rename(columns=_prefixed_map("opt_", metric_cols)).drop(
        columns=[K.QUERY_NAME]
    )

    joined = base.merge(opt, on=[K.STEP_KEY, K.ITERATION], how="outer", indicator=True)
    joined = _add_present_in(joined)

    # Calculation 1: Mean
    base_mean = joined.get(K.BASE_MEAN_MS)
    opt_mean = joined.get(K.OPT_MEAN_MS)

    # We check type to satisfy linters, though logically they should be Series
    if isinstance(base_mean, pd.Series) and isinstance(opt_mean, pd.Series):
        # FIX: Use replace(0.0, np.nan) instead of the dict with pd.NA
        # This keeps the dtype as float and satisfies the type checker.
        denominator = base_mean.replace(0.0, np.nan)
        joined[K.OPT_OVER_BASE_MEAN] = opt_mean / denominator
        joined[K.DIFF_MEAN_MS] = opt_mean - base_mean

    # Calculation 2: Median
    base_med = joined.get(K.BASE_MEDIAN_MS)
    opt_med = joined.get(K.OPT_MEDIAN_MS)

    if isinstance(base_med, pd.Series) and isinstance(opt_med, pd.Series):
        # FIX: Consistent fix here as well
        denominator = base_med.replace(0.0, np.nan)
        joined[K.OPT_OVER_BASE_MEDIAN] = opt_med / denominator
        joined[K.DIFF_MEDIAN_MS] = opt_med - base_med

    # Sorting
    joined = joined.sort_values(
        by=K.OPT_OVER_BASE_MEDIAN,
        ascending=False,
        kind="mergesort",
        na_position="last",
    )
    joined = joined.sort_values(by=K.PRESENT_IN, ascending=True, kind="mergesort")

    return joined.reset_index(drop=True)
