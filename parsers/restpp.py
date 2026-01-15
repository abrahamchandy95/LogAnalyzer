from math import nan
from pathlib import Path
from typing import NotRequired, TypedDict, TypeIs

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

from common.regexes import QUERY_ENDPOINT_RE, RETURNRESULT_RE
from common.types import RunKey
from common.utils import detect_year_from_header, parse_glog_line


# --- Type Definitions ---

type Timestampish = pd.Timestamp | NaTType


def is_real_timestamp(x: Timestampish) -> TypeIs[pd.Timestamp]:
    return not isinstance(x, NaTType)


class _RestppRow(TypedDict):
    run: RunKey
    node: str
    ts: pd.Timestamp
    tid: int
    request_id: str

    method: NotRequired[str | None]
    endpoint: NotRequired[str | None]
    query_name: NotRequired[str | None]
    graph_name: NotRequired[str | None]

    restpp_return_ms: NotRequired[float]
    restpp_engine: NotRequired[str | None]
    return_ts: NotRequired[Timestampish]

    log_path: str
    lineno: int


# --- Output Contract (Stable Schema) ---

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

# Prevent schema drift from RequestInfo|, (add more only intentionally)
_ALLOWED_REQINFO_KEYS: frozenset[str] = frozenset({"graph_name"})


# --- Parsing Helpers ---


def _parse_raw_request(
    msg: str,
) -> tuple[str, str | None, str | None, str | None] | None:
    if "RawRequest|," not in msg:
        return None
    try:
        after = msg.split("RawRequest|,", 1)[1]
        request_id, rest = after.split(",", 1)
    except ValueError:
        return None

    parts = rest.split("|")
    method = parts[1] if len(parts) > 1 else None
    endpoint = parts[2] if len(parts) > 2 else None

    qname: str | None = None
    if endpoint:
        m_q = QUERY_ENDPOINT_RE.search(endpoint)
        if m_q:
            qname = m_q.group("qname")

    return request_id.strip(), method, endpoint, qname


def _parse_return_result(msg: str) -> tuple[str, float, str] | None:
    m_rr = RETURNRESULT_RE.search(msg)
    if not m_rr:
        return None
    return (
        m_rr.group("rid").strip(),
        float(m_rr.group("ms")),
        m_rr.group("engine"),
    )


def _parse_request_info(msg: str) -> tuple[str, dict[str, str]] | None:
    if "RequestInfo|," not in msg:
        return None

    try:
        after = msg.split("RequestInfo|,", 1)[1]
        request_id, rest = after.split(",", 1)
    except ValueError:
        return None

    kv: dict[str, str] = {}
    for p in rest.split("|"):
        if ":" in p:
            k, v = p.split(":", 1)
            key = k.strip()
            if key in _ALLOWED_REQINFO_KEYS:
                kv[key] = v.strip()

    return request_id.strip(), kv


# --- Row Builders (SRP: constructing rows) ---


def _make_raw_row(
    *,
    run: RunKey,
    node: str,
    ts: pd.Timestamp,
    tid: int,
    log_path: Path,
    lineno: int,
    request_id: str,
    method: str | None,
    endpoint: str | None,
    query_name: str | None,
) -> _RestppRow:
    return {
        "run": run,
        "node": node,
        "ts": ts,
        "tid": tid,
        "log_path": str(log_path),
        "lineno": lineno,
        "request_id": request_id,
        "method": method,
        "endpoint": endpoint,
        "query_name": query_name,
        "restpp_return_ms": nan,
        "restpp_engine": None,
        "return_ts": pd.NaT,
    }


def _make_return_row(
    *,
    run: RunKey,
    node: str,
    ts: pd.Timestamp,
    tid: int,
    log_path: Path,
    lineno: int,
    request_id: str,
    return_ms: float,
    engine: str,
) -> _RestppRow:
    return {
        "run": run,
        "node": node,
        "ts": ts,
        "tid": tid,
        "log_path": str(log_path),
        "lineno": lineno,
        "request_id": request_id,
        "restpp_return_ms": return_ms,
        "restpp_engine": engine,
        "return_ts": ts,
    }


# --- Aggregation Helper ---


def _first_str(s: pd.Series) -> str | None:
    # Defensive: pandas stubs treat element type as Unknown
    for x in s:
        if isinstance(x, str):
            return x
    return None


def _aggregate_events(
    rows: list[_RestppRow], reqinfo: dict[str, dict[str, str]]
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=_OUT_COLS)

    df = pd.DataFrame(rows)

    # Merge RequestInfo (controlled keys only)
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

    # Enforce stable schema + order (future-proof)
    agg = agg.reindex(columns=_OUT_COLS)

    # Typed-friendly sort without sort_values(list[str]) overload
    agg = agg.set_index(["run", "restpp_ts"]).sort_index().reset_index()
    return agg.reset_index(drop=True)


# --- Main Parser ---


def parse_restpp(
    run_key: RunKey, run_dir: Path, *, nodes: tuple[str, ...]
) -> pd.DataFrame:
    rows: list[_RestppRow] = []
    reqinfo: dict[str, dict[str, str]] = {}

    folder_year = int(run_key.split("-", 1)[0]) if "-" in run_key else 2026

    for node in nodes:
        node_dir = run_dir / node
        if not node_dir.exists():
            continue

        for log_path in node_dir.glob("restpp*"):
            if not log_path.is_file():
                continue

            file_year = detect_year_from_header(log_path, default_year=folder_year)

            with log_path.open("r", errors="replace") as f:
                for lineno, line in enumerate(f, start=1):
                    if line.startswith(">>>>>>>"):
                        continue

                    gl = parse_glog_line(line, year=file_year)
                    if not gl:
                        continue

                    ts_candidate = pd.Timestamp(gl.ts)
                    if not is_real_timestamp(ts_candidate):
                        continue
                    ts: pd.Timestamp = ts_candidate

                    msg = gl.msg

                    raw = _parse_raw_request(msg)
                    if raw is not None:
                        rid, method, endpoint, qname = raw
                        rows.append(
                            _make_raw_row(
                                run=run_key,
                                node=node,
                                ts=ts,
                                tid=gl.tid,
                                log_path=log_path,
                                lineno=lineno,
                                request_id=rid,
                                method=method,
                                endpoint=endpoint,
                                query_name=qname,
                            )
                        )
                        continue

                    rr = _parse_return_result(msg)
                    if rr is not None:
                        rid, ms, engine = rr
                        rows.append(
                            _make_return_row(
                                run=run_key,
                                node=node,
                                ts=ts,
                                tid=gl.tid,
                                log_path=log_path,
                                lineno=lineno,
                                request_id=rid,
                                return_ms=ms,
                                engine=engine,
                            )
                        )
                        continue

                    info = _parse_request_info(msg)
                    if info is not None:
                        rid, kv = info
                        if kv:
                            reqinfo.setdefault(rid, {}).update(kv)

    return _aggregate_events(rows, reqinfo)
