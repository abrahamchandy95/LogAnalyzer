import pandas as pd

from analysis.dfutils import as_df, filter_notna
from common.model.constants import GPE_UDF_START, GPE_UDF_STOP

from analysis.dfkeys import (
    RUN,
    REQUEST_ID,
    NODE,
    EVENT,
    TS,
    UDF_MS,
    GPE_NODE,
    FIRST_SEEN_GPE_TS,
    LAST_SEEN_GPE_TS,
    START_UDF_TS,
    STOP_UDF_TS,
    REPORTED_STOP_UDF_MS,
    ACTUAL_STOP_UDF_MS,
    ACTUAL_DIFF_FIRST_LAST_SEEN_MS,
    DIFF_GPE_DURATION_UDF_MS,
)
from .util import first_str, elapsed_ms


_GROUP_KEYS = [RUN, REQUEST_ID]


def udf_boundaries(g: pd.DataFrame) -> pd.DataFrame:
    """
    SRP: compute start/stop/reported Stop_RunUDF per (run, request_id).
    DRY: used by BOTH summarize_requests() and build_exec_request_table().
    """
    if g.empty:
        return pd.DataFrame(
            columns=pd.Index(
                [RUN, REQUEST_ID, START_UDF_TS, STOP_UDF_TS, REPORTED_STOP_UDF_MS]
            )
        )

    starts = (
        g.loc[g[EVENT] == GPE_UDF_START]
        .groupby(_GROUP_KEYS)[TS]
        .min()
        .rename(START_UDF_TS)
    )
    stops = (
        g.loc[g[EVENT] == GPE_UDF_STOP]
        .groupby(_GROUP_KEYS)[TS]
        .max()
        .rename(STOP_UDF_TS)
    )
    reported = (
        g.loc[g[EVENT] == GPE_UDF_STOP]
        .groupby(_GROUP_KEYS)[UDF_MS]
        .max()
        .rename(REPORTED_STOP_UDF_MS)
    )

    return as_df(pd.concat([starts, stops, reported], axis=1).reset_index())


def summarize_gpe_per_request(gpe_attached: pd.DataFrame) -> pd.DataFrame:
    """
    SRP: build the GPE-side per-request summary table.
    """
    g = filter_notna(gpe_attached, REQUEST_ID)
    if g.empty:
        return pd.DataFrame(
            columns=pd.Index(
                [
                    RUN,
                    REQUEST_ID,
                    GPE_NODE,
                    FIRST_SEEN_GPE_TS,
                    LAST_SEEN_GPE_TS,
                    START_UDF_TS,
                    STOP_UDF_TS,
                    REPORTED_STOP_UDF_MS,
                    ACTUAL_STOP_UDF_MS,
                    ACTUAL_DIFF_FIRST_LAST_SEEN_MS,
                    DIFF_GPE_DURATION_UDF_MS,
                ]
            )
        )

    base = as_df(
        g.groupby(_GROUP_KEYS, as_index=False).agg(
            **{
                GPE_NODE: (NODE, first_str),
                FIRST_SEEN_GPE_TS: (TS, "min"),
                LAST_SEEN_GPE_TS: (TS, "max"),
            }
        )
    )

    bounds = udf_boundaries(g)
    out = as_df(base.merge(bounds, on=_GROUP_KEYS, how="left"))

    out = elapsed_ms(
        out, start_col=START_UDF_TS, stop_col=STOP_UDF_TS, out_col=ACTUAL_STOP_UDF_MS
    )
    out = elapsed_ms(
        out,
        start_col=FIRST_SEEN_GPE_TS,
        stop_col=LAST_SEEN_GPE_TS,
        out_col=ACTUAL_DIFF_FIRST_LAST_SEEN_MS,
    )
    out[DIFF_GPE_DURATION_UDF_MS] = (
        out[ACTUAL_DIFF_FIRST_LAST_SEEN_MS] - out[ACTUAL_STOP_UDF_MS]
    )

    return out
