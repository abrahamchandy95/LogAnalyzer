import pandas as pd

from analysis.dfutils import as_df, filter_notna
from analysis.dfkeys import (
    RUN,
    REQUEST_ID,
    START_UDF_TS,
    STOP_UDF_TS,
    FIRST_SEEN_GPE_TS,
    ACTUAL_STOP_UDF_MS,
)
from .gpe_rollup import summarize_gpe_per_request, udf_boundaries
from .restpp_rollup import (
    summarize_restpp_per_request,
    restpp_request_map,
    add_endpoint_name,
)
from .util import elapsed_ms
from .columns import EXEC_REQUEST_TABLE_COLS


def summarize_requests(
    restpp_req: pd.DataFrame, gpe_attached: pd.DataFrame
) -> pd.DataFrame:
    gpe_sum = summarize_gpe_per_request(gpe_attached)
    rsum = summarize_restpp_per_request(restpp_req)

    out = as_df(gpe_sum.merge(rsum, on=[RUN, REQUEST_ID], how="left"))
    out = add_endpoint_name(out)

    if FIRST_SEEN_GPE_TS in out.columns:
        out = out.set_index([RUN, FIRST_SEEN_GPE_TS]).sort_index().reset_index()

    return out.reset_index(drop=True)


def build_exec_request_table(
    restpp_req: pd.DataFrame, gpe_attached: pd.DataFrame
) -> pd.DataFrame:
    g = filter_notna(gpe_attached, REQUEST_ID)
    if g.empty:
        return pd.DataFrame(columns=EXEC_REQUEST_TABLE_COLS)

    bounds = udf_boundaries(g)

    exec_tbl = bounds.dropna(subset=[START_UDF_TS, STOP_UDF_TS]).copy()
    exec_tbl = elapsed_ms(
        exec_tbl,
        start_col=START_UDF_TS,
        stop_col=STOP_UDF_TS,
        out_col=ACTUAL_STOP_UDF_MS,
    )

    rmap = restpp_request_map(restpp_req)
    if not rmap.empty:
        keep = [
            c
            for c in [RUN, REQUEST_ID, "query_name", "graph_name", "endpoint"]
            if c in rmap.columns
        ]
        exec_tbl = as_df(
            exec_tbl.merge(rmap.loc[:, keep], on=[RUN, REQUEST_ID], how="left")
        )

    exec_tbl = exec_tbl.set_index([RUN, START_UDF_TS]).sort_index().reset_index()
    return exec_tbl.reset_index(drop=True)


def extract_ids(
    exec_tbl: pd.DataFrame, base_q: str, opt_q: str
) -> tuple[list[str], list[str]]:
    # Keeping this simple; you *could* move these col keys to schema too if you want.
    base_ids = (
        exec_tbl.loc[exec_tbl["query_name"] == base_q, REQUEST_ID]
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()
    )
    opt_ids = (
        exec_tbl.loc[exec_tbl["query_name"] == opt_q, REQUEST_ID]
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()
    )
    return base_ids, opt_ids
