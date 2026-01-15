from math import nan
from pathlib import Path
from typing import NotRequired, TypedDict, TypeIs

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

from common.regexes import QUERY_ENDPOINT_RE, RETURNRESULT_RE
from common.types import RunKey
from common.utils import detect_year_from_header, parse_glog_line


type Timestampish = pd.Timestamp | NaTType


def is_real_timestamp(x: Timestampish) -> TypeIs[pd.Timestamp]:
    """Type-narrow helper: pandas stubs say pd.Timestamp(...) may return NaTType."""
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

                    parsed = parse_glog_line(line, year=file_year)
                    if not parsed:
                        continue

                    ts_candidate = pd.Timestamp(parsed.ts)
                    if not is_real_timestamp(ts_candidate):
                        continue
                    ts: pd.Timestamp = ts_candidate

                    tid = parsed.tid
                    msg = parsed.msg

                    if "RawRequest|," in msg:
                        try:
                            after = msg.split("RawRequest|,", 1)[1]
                            request_id, rest = after.split(",", 1)
                        except ValueError:
                            continue

                        parts = rest.split("|")
                        method = parts[1] if len(parts) > 1 else None
                        endpoint = parts[2] if len(parts) > 2 else None

                        qname: str | None = None
                        if endpoint:
                            m_q = QUERY_ENDPOINT_RE.search(endpoint)
                            if m_q:
                                qname = m_q.group("qname")

                        request_row: _RestppRow = {
                            "run": run_key,
                            "node": node,
                            "ts": ts,
                            "tid": tid,
                            "request_id": request_id.strip(),
                            "method": method,
                            "endpoint": endpoint,
                            "query_name": qname,
                            "restpp_return_ms": nan,
                            "restpp_engine": None,
                            "return_ts": pd.NaT,
                            "log_path": str(log_path),
                            "lineno": lineno,
                        }
                        rows.append(request_row)
                        continue

                    if "RequestInfo|," in msg:
                        try:
                            after = msg.split("RequestInfo|,", 1)[1]
                            request_id, rest = after.split(",", 1)
                        except ValueError:
                            continue

                        kv: dict[str, str] = {}
                        for p in rest.split("|"):
                            if ":" in p:
                                k, v = p.split(":", 1)
                                kv[k.strip()] = v.strip()

                        reqinfo.setdefault(request_id.strip(), {}).update(kv)
                        continue

                    m_rr = RETURNRESULT_RE.search(msg)
                    if m_rr:
                        rid = m_rr.group("rid").strip()
                        return_row: _RestppRow = {
                            "run": run_key,
                            "node": node,
                            "ts": ts,
                            "tid": tid,
                            "request_id": rid,
                            "restpp_return_ms": float(m_rr.group("ms")),
                            "restpp_engine": m_rr.group("engine"),
                            "return_ts": ts,
                            "log_path": str(log_path),
                            "lineno": lineno,
                        }
                        rows.append(return_row)

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=pd.Index(
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
        )

    if reqinfo:
        info_df = pd.DataFrame([{"request_id": k, **v} for k, v in reqinfo.items()])
        if not info_df.empty:
            df = df.merge(info_df, on="request_id", how="left")

    def _first_str(s: pd.Series) -> str | None:
        return next((x for x in s if isinstance(x, str)), None)

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

    # Typed-friendly multi-key sort without pandas' sort_values overload headaches
    agg = agg.set_index(["run", "restpp_ts"]).sort_index().reset_index()

    return agg.reset_index(drop=True)
