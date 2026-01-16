import pandas as pd

from analysis.dfutils import as_df
from common.parse.regexes import QUERY_ENDPOINT_RE

from analysis.dfkeys import (
    RUN,
    REQUEST_ID,
    RESTPP_NODE,
    RESTPP_TS,
    RESTPP_RETURN_TS,
    RESTPP_RETURN_MS,
    QUERY_NAME,
    GRAPH_NAME,
    ENDPOINT,
    FIRST_SEEN_TS_RESTPP,
    SYNC_RETURN_TS_RESTPP,
    SYNC_RETURN_TIME_MS,
    FULL_ENDPOINT,
    ENDPOINT_NAME,
)


_RENAME = {
    RESTPP_TS: FIRST_SEEN_TS_RESTPP,
    RESTPP_RETURN_TS: SYNC_RETURN_TS_RESTPP,
    RESTPP_RETURN_MS: SYNC_RETURN_TIME_MS,
    ENDPOINT: FULL_ENDPOINT,
}


def ensure_restpp_cols(restpp_req: pd.DataFrame) -> pd.DataFrame:
    r = restpp_req.copy()
    if RESTPP_RETURN_TS not in r.columns:
        r[RESTPP_RETURN_TS] = pd.NaT
    return r


def restpp_request_map(restpp_req: pd.DataFrame) -> pd.DataFrame:
    r = ensure_restpp_cols(restpp_req)

    wanted = [
        RUN,
        REQUEST_ID,
        QUERY_NAME,
        GRAPH_NAME,
        ENDPOINT,
        RESTPP_NODE,
        RESTPP_TS,
        RESTPP_RETURN_TS,
        RESTPP_RETURN_MS,
    ]
    keep = [c for c in wanted if c in r.columns]
    if not keep:
        return pd.DataFrame(columns=pd.Index([RUN, REQUEST_ID]))

    rsel = as_df(r.loc[:, keep].copy())
    # single row per request (you can choose which “wins” by sorting before drop_duplicates if needed)
    return rsel.drop_duplicates(subset=[RUN, REQUEST_ID])


def summarize_restpp_per_request(restpp_req: pd.DataFrame) -> pd.DataFrame:
    rmap = restpp_request_map(restpp_req)
    if rmap.empty:
        return rmap

    return rmap.rename(columns=_RENAME)


def add_endpoint_name(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    fe = out.get(FULL_ENDPOINT)
    if isinstance(fe, pd.Series):
        out[ENDPOINT_NAME] = fe.astype("string").str.extract(
            QUERY_ENDPOINT_RE, expand=False
        )
    else:
        out[ENDPOINT_NAME] = pd.Series(dtype="string")
    return out
