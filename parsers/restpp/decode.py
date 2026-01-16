from math import nan

import pandas as pd

from common.model.constants import (
    RESTPP_RAW_TOKEN,
    RESTPP_REQINFO_TOKEN,
    REQINFO_ALLOWED_KEYS,
)
from common.parse.regexes import QUERY_ENDPOINT_RE, RETURNRESULT_RE
from parsers._walker import ParsedLine

from .records import (
    RawRequestParsed,
    ReturnResultParsed,
    RequestInfoParsed,
    RestppRawRecord,
    RestppReturnRecord,
    RestppInfoRecord,
    RestppRecord,
    RestppRow,
)


def parse_raw_request(msg: str) -> RawRequestParsed | None:
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

    qname = None
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


def parse_return_result(msg: str) -> ReturnResultParsed | None:
    m_rr = RETURNRESULT_RE.search(msg)
    if not m_rr:
        return None

    return ReturnResultParsed(
        request_id=m_rr.group("rid").strip(),
        ms=float(m_rr.group("ms")),
        engine=m_rr.group("engine"),
    )


def parse_request_info(msg: str) -> RequestInfoParsed | None:
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


def classify_msg(msg: str) -> RestppRecord | None:
    raw = parse_raw_request(msg)
    if raw is not None:
        return RestppRawRecord(parsed=raw)

    rr = parse_return_result(msg)
    if rr is not None:
        return RestppReturnRecord(parsed=rr)

    info = parse_request_info(msg)
    if info is not None:
        return RestppInfoRecord(parsed=info)

    return None


def make_raw_row(*, pl: ParsedLine, parsed: RawRequestParsed) -> RestppRow:
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


def make_return_row(*, pl: ParsedLine, parsed: ReturnResultParsed) -> RestppRow:
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
