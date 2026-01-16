import numpy as np
import pandas as pd

from analysis import dfkeys as K
from analysis.dfutils import as_df, safe_div  # Imported safe_div
from analysis.step_stats.schema import STEP_STATS_COLS
from common.model.constants import GPE_STEP
from common.support.stats import pct


def _p50(s: pd.Series) -> float:
    return pct(s, 50)


def _p95(s: pd.Series) -> float:
    return pct(s, 95)


def make_step_stats(gaps_with_qname: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate gap_ms per (query_name, step_key, iteration).
    """
    if gaps_with_qname.empty:
        return pd.DataFrame(columns=STEP_STATS_COLS)

    qn = gaps_with_qname.get(K.QUERY_NAME)
    if not isinstance(qn, pd.Series):
        return pd.DataFrame(columns=STEP_STATS_COLS)

    g = as_df(gaps_with_qname.loc[pd.notna(qn)].copy())
    if g.empty:
        return pd.DataFrame(columns=STEP_STATS_COLS)

    grp = g.groupby([K.QUERY_NAME, K.STEP_KEY, K.ITERATION], dropna=False)

    out = as_df(
        grp.agg(
            n=(K.GAP_MS, "count"),
            median_ms=(K.GAP_MS, _p50),
            p95_ms=(K.GAP_MS, _p95),
            mean_ms=(K.GAP_MS, "mean"),
            max_ms=(K.GAP_MS, "max"),
            sum_ms=(K.GAP_MS, "sum"),
        ).reset_index()
    )

    # stable two-pass sort
    out = out.sort_values(by=K.SUM_MS, ascending=False, kind="mergesort")
    out = out.sort_values(by=K.QUERY_NAME, ascending=True, kind="mergesort")
    return out.reindex(columns=STEP_STATS_COLS).reset_index(drop=True)


def build_ordered_step_side_table(
    gapsq: pd.DataFrame,
    *,
    base_query: str,
    opt_query: str,
    step_prefix: str = "Step ",
) -> pd.DataFrame:
    """
    Side-by-side stats for steps, ordered by median position within each request.
    Uses only STEP events.
    """
    if gapsq.empty:
        return pd.DataFrame()

    # Filter to the two queries and STEP events
    qn = gapsq.get(K.QUERY_NAME)
    ev = gapsq.get(K.EVENT)
    if not isinstance(qn, pd.Series) or not isinstance(ev, pd.Series):
        return pd.DataFrame()

    mask = qn.isin([base_query, opt_query]) & (ev == GPE_STEP)
    twoq = as_df(gapsq.loc[mask].copy())
    if twoq.empty:
        return pd.DataFrame()

    sk = twoq.get(K.STEP_KEY)
    if not isinstance(sk, pd.Series):
        return pd.DataFrame()

    twoq = as_df(twoq.loc[sk.astype(str).str.startswith(step_prefix)].copy())
    if twoq.empty:
        return pd.DataFrame()

    # Deterministic ordering for cumcount
    twoq = (
        twoq.set_index([K.QUERY_NAME, K.RUN, K.REQUEST_ID, K.TS])
        .sort_index()
        .reset_index()
    )
    twoq[K.POS_IN_REQUEST] = (
        twoq.groupby([K.QUERY_NAME, K.RUN, K.REQUEST_ID]).cumcount() + 1
    )

    # Median position per step_key within each query
    pos_tbl = (
        twoq.groupby([K.QUERY_NAME, K.STEP_KEY])[K.POS_IN_REQUEST]
        .median()
        .rename(K.MEDIAN_POS)
        .reset_index()
    )

    # Per (query_name, step_key) stats
    stats = (
        twoq.groupby([K.QUERY_NAME, K.STEP_KEY])
        .agg(
            n=(K.GAP_MS, "size"),
            mean_ms=(K.GAP_MS, "mean"),
            median_ms=(K.GAP_MS, _p50),
            p95_ms=(K.GAP_MS, _p95),
            max_ms=(K.GAP_MS, "max"),
            sum_ms=(K.GAP_MS, "sum"),
        )
        .reset_index()
        .merge(pos_tbl, on=[K.QUERY_NAME, K.STEP_KEY], how="left")
    )

    # Split + prefix
    base = stats.loc[stats[K.QUERY_NAME] == base_query].copy()
    opt = stats.loc[stats[K.QUERY_NAME] == opt_query].copy()

    # DRY Note: These dicts are explicit because key mapping (MEDIAN_POS -> BASE_POS)
    # isn't a strict prefix rule. Explicit is better than implicit here.
    base = base.rename(
        columns={
            K.N: K.BASE_N,
            K.MEAN_MS: K.BASE_MEAN_MS,
            K.MEDIAN_MS: K.BASE_MEDIAN_MS,
            K.P95_MS: K.BASE_P95_MS,
            K.MAX_MS: K.BASE_MAX_MS,
            K.SUM_MS: K.BASE_SUM_MS,
            K.MEDIAN_POS: K.BASE_POS,
        }
    ).drop(columns=[K.QUERY_NAME])

    opt = opt.rename(
        columns={
            K.N: K.OPT_N,
            K.MEAN_MS: K.OPT_MEAN_MS,
            K.MEDIAN_MS: K.OPT_MEDIAN_MS,
            K.P95_MS: K.OPT_P95_MS,
            K.MAX_MS: K.OPT_MAX_MS,
            K.SUM_MS: K.OPT_SUM_MS,
            K.MEDIAN_POS: K.OPT_POS,
        }
    ).drop(columns=[K.QUERY_NAME])

    side = base.merge(opt, on=K.STEP_KEY, how="outer", indicator=True)

    merge_s = side["_merge"].astype("string")
    side[K.PRESENT_IN] = merge_s.replace(
        {"left_only": "base_only", "right_only": "opt_only", "both": "both"}
    )
    side = side.drop(columns=["_merge"])

    # Ordering
    side[K.POS] = side[[K.BASE_POS, K.OPT_POS]].median(axis=1, skipna=True)
    side = side.sort_values([K.POS, K.PRESENT_IN], na_position="last").reset_index(
        drop=True
    )

    # Convenience columns used by plotting/export
    side[K.SELECT_LIKE_IDX] = np.arange(1, len(side) + 1)

    base_mean = side.get(K.BASE_MEAN_MS)
    opt_mean = side.get(K.OPT_MEAN_MS)
    if isinstance(base_mean, pd.Series) and isinstance(opt_mean, pd.Series):
        side[K.DIFF_MEAN_MS] = opt_mean - base_mean
        # FIX: Replaced manual logic with safe_div helper
        side[K.OPT_OVER_BASE_MEAN] = safe_div(opt_mean, base_mean)

    return side
