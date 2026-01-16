import pandas as pd

from analysis.dfkeys import (
    QUERY_NAME,
    GAP_MS,
    STEP_KEY,
    ITERATION,
    RUN,
    NODE,
    TID,
    REQUEST_ID,
    TS,
    PREV_LABEL,
    EVENT,
    LABEL,
    LOG_PATH,
    LINENO,
    DETAIL,
)

_BOTTLENECK_COLS: tuple[str, ...] = (
    QUERY_NAME,
    GAP_MS,
    STEP_KEY,
    ITERATION,
    RUN,
    NODE,
    TID,
    REQUEST_ID,
    TS,
    PREV_LABEL,
    EVENT,
    LABEL,
    LOG_PATH,
    LINENO,
    DETAIL,
)


def _empty_bottlenecks_df() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(_BOTTLENECK_COLS))


def _existing_cols(df: pd.DataFrame, cols: tuple[str, ...]) -> list[str]:
    return [c for c in cols if c in df.columns]


def top_bottlenecks(
    gaps_with_qname: pd.DataFrame, query_name: str, *, n: int = 50
) -> pd.DataFrame:
    """
    Top slowest individual gaps for a given query variant.
    Includes (log_path, lineno) to jump to the exact log line.
    """
    if gaps_with_qname.empty or QUERY_NAME not in gaps_with_qname.columns:
        return _empty_bottlenecks_df()

    mask = gaps_with_qname[QUERY_NAME] == query_name
    g = gaps_with_qname.loc[mask].copy()

    if g.empty:
        return _empty_bottlenecks_df()

    keep = _existing_cols(g, _BOTTLENECK_COLS)

    if GAP_MS in g.columns:
        g = g.sort_values(GAP_MS, ascending=False, kind="mergesort")

    return g.head(n)[keep].reset_index(drop=True)
