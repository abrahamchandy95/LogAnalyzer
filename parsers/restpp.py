from dataclasses import dataclass
from math import nan
from pathlib import Path
from typing import NotRequired, TypedDict

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

from common.parse.regexes import QUERY_ENDPOINT_RE, RETURNRESULT_RE
from common.model.types import QueryName, RequestId, RunId
from common.model.constants import (
    RESTPP_RAW_TOKEN,
    RESTPP_REQINFO_TOKEN,
    REQINFO_ALLOWED_KEYS,
    RESTPP_GLOB,
)
from parsers._walker import ParsedLine, walk_logs


type Timestampish = pd.Timestamp | NaTType


@dataclass(frozen=True, slots=True)
class RawRequestParsed:
    request_id: RequestId
    method: str | None
    endpoint: str | None
    query_name: QueryName | None


@dataclass(frozen=True, slots=True)
class ReturnResultParsed:
    request_id: RequestId
    ms: float
    engine: str


@dataclass(frozen=True, slots=True)
class RequestInfoParsed:
    request_id: RequestId
    kv: dict[str, str]


@dataclass(frozen=True, slots=True)
class RestppRawRecord:
    parsed: RawRequestParsed


@dataclass(frozen=True, slots=True)
class RestppReturnRecord:
    parsed: ReturnResultParsed


@dataclass(frozen=True, slots=True)
class RestppInfoRecord:
    parsed: RequestInfoParsed


type RestppRecord = RestppRawRecord | RestppReturnRecord | RestppInfoRecord


class _RestppRow(TypedDict):
    run: RunId
    node: str
    ts: pd.Timestamp
    tid: int
    request_id: RequestId

    method: NotRequired[str | None]
    endpoint: NotRequired[str | None]
    query_name: NotRequired[QueryName | None]
    graph_name: NotRequired[str | None]

    restpp_return_ms: NotRequired[float]
    restpp_engine: NotRequired[str | None]
    return_ts: NotRequired[Timestampish]

    log_path: str
    lineno: int


_OUT_COLS = pd.Index(
    [
        "run",
        "request_id",
        "restpp_ts",
        "restpp_node",
        "endpoint",
        "query_name",
        "graph_name",
        "restpp_return_ms",
        "restpp_engine",
        "restpp_return_ts",
    ]
)


# Parsing helpers
def _parse_raw_request(msg: str) -> RawRequestParsed | None:
    if RESTPP_RAW_TOKEN not in msg:
        return None

    try:
        after = msg.split(RESTPP_RAW_TOKEN, 1)[1]
        request_id, rest = after.split(",", 1)
    except ValueError:
        return None

    parts = rest.split("|")
    method = parts[1] if len(parts) > 1 else None
    endpoint = parts[2] if len(parts) > 2 else None

    qname: QueryName | None = None
    if endpoint:
        m_q = QUERY_ENDPOINT_RE.search(endpoint)
        if m_q:
            qname = m_q.group("qname")

    return RawRequestParsed(
        request_id=request_id.strip(),
        method=method,
        endpoint=endpoint,
        query_name=qname,
    )


def _parse_return_result(msg: str) -> ReturnResultParsed | None:
    m_rr = RETURNRESULT_RE.search(msg)
    if not m_rr:
        return None
    return ReturnResultParsed(
        request_id=m_rr.group("rid").strip(),
        ms=float(m_rr.group("ms")),
        engine=m_rr.group("engine"),
    )


def _parse_request_info(msg: str) -> RequestInfoParsed | None:
    if RESTPP_REQINFO_TOKEN not in msg:
        return None

    try:
        after = msg.split(RESTPP_REQINFO_TOKEN, 1)[1]
        request_id, rest = after.split(",", 1)
    except ValueError:
        return None

    kv: dict[str, str] = {}
    for p in rest.split("|"):
        if ":" in p:
            k, v = p.split(":", 1)
            key = k.strip()
            if key in REQINFO_ALLOWED_KEYS:
                kv[key] = v.strip()

    return RequestInfoParsed(request_id=request_id.strip(), kv=kv)


def _classify_msg(msg: str) -> RestppRecord | None:
    """
    Classifies a RESTPP log message into one of the supported record types.
    """
    raw = _parse_raw_request(msg)
    if raw is not None:
        return RestppRawRecord(parsed=raw)

    rr = _parse_return_result(msg)
    if rr is not None:
        return RestppReturnRecord(parsed=rr)

    info = _parse_request_info(msg)
    if info is not None:
        return RestppInfoRecord(parsed=info)

    return None


# --- Row builders ---


def _make_raw_row(*, pl: ParsedLine, parsed: RawRequestParsed) -> _RestppRow:
    return {
        "run": pl.run,
        "node": pl.node,
        "ts": pl.ts,
        "tid": pl.tid,
        "log_path": str(pl.log_path),
        "lineno": pl.lineno,
        "request_id": parsed.request_id,
        "method": parsed.method,
        "endpoint": parsed.endpoint,
        "query_name": parsed.query_name,
        "restpp_return_ms": nan,
        "restpp_engine": None,
        "return_ts": pd.NaT,
    }


def _make_return_row(*, pl: ParsedLine, parsed: ReturnResultParsed) -> _RestppRow:
    return {
        "run": pl.run,
        "node": pl.node,
        "ts": pl.ts,
        "tid": pl.tid,
        "log_path": str(pl.log_path),
        "lineno": pl.lineno,
        "request_id": parsed.request_id,
        "restpp_return_ms": parsed.ms,
        "restpp_engine": parsed.engine,
        "return_ts": pl.ts,
    }


def _first_str(s: pd.Series) -> str | None:
    for x in s:
        if isinstance(x, str):
            return x
    return None


def _aggregate_events(
    rows: list[_RestppRow], reqinfo: dict[RequestId, dict[str, str]]
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=_OUT_COLS)

    df = pd.DataFrame(rows)

    if reqinfo:
        info_df = pd.DataFrame([{"request_id": k, **v} for k, v in reqinfo.items()])
        if not info_df.empty:
            df = df.merge(info_df, on="request_id", how="left")

    agg = df.groupby(["run", "request_id"], as_index=False).agg(
        restpp_ts=("ts", "min"),
        restpp_node=("node", "first"),
        endpoint=("endpoint", _first_str),
        query_name=("query_name", _first_str),
        graph_name=("graph_name", _first_str),
        restpp_return_ms=("restpp_return_ms", "max"),
        restpp_engine=("restpp_engine", _first_str),
        restpp_return_ts=("return_ts", "max"),
    )

    agg = agg.reindex(columns=_OUT_COLS)
    agg = agg.set_index(["run", "restpp_ts"]).sort_index().reset_index()
    return agg.reset_index(drop=True)


# --- Main parser ---


def parse_restpp(
    run_id: RunId, run_dir: Path, *, nodes: tuple[str, ...]
) -> pd.DataFrame:
    rows: list[_RestppRow] = []
    reqinfo: dict[RequestId, dict[str, str]] = {}

    def _handle(pl: ParsedLine) -> None:
        rec = _classify_msg(pl.msg)
        if rec is None:
            return

        match rec:
            case RestppRawRecord(parsed=raw):
                rows.append(_make_raw_row(pl=pl, parsed=raw))
            case RestppReturnRecord(parsed=rr):
                rows.append(_make_return_row(pl=pl, parsed=rr))
            case RestppInfoRecord(parsed=info):
                if info.kv:
                    reqinfo.setdefault(info.request_id, {}).update(info.kv)

    walk_logs(
        run_id=run_id,
        run_dir=run_dir,
        nodes=nodes,
        file_glob=RESTPP_GLOB,
        on_line=_handle,
    )

    return _aggregate_events(rows, reqinfo)
