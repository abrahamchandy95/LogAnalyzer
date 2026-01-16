import re
from dataclasses import dataclass


def _compile(pattern: str, flags: int = 0) -> re.Pattern[str]:
    return re.compile(pattern, flags)


@dataclass(frozen=True, slots=True)
class _GlogRegexes:
    info_line: re.Pattern[str]
    header_date: re.Pattern[str]


@dataclass(frozen=True, slots=True)
class _RestppRegexes:
    query_endpoint: re.Pattern[str]
    return_result: re.Pattern[str]
    req_id: re.Pattern[str]


@dataclass(frozen=True, slots=True)
class _GpeRegexes:
    start_runudf: re.Pattern[str]
    stop_runudf: re.Pattern[str]
    udf_step: re.Pattern[str]
    iter_in_detail: re.Pattern[str]


@dataclass(frozen=True, slots=True)
class _RequestIdRegexes:
    epoch_ms: re.Pattern[str]


GLOG = _GlogRegexes(
    info_line=_compile(
        r"^I(?P<mm>\d{2})(?P<dd>\d{2})\s+(?P<hms>\d{2}:\d{2}:\d{2}\.\d+)\s+(?P<tid>\d+)\s+.*?\]\s+(?P<msg>.*)$"
    ),
    header_date=_compile(r"INFO\.(?P<year>\d{4})(?P<mmdd>\d{4})"),
)

RESTPP = _RestppRegexes(
    query_endpoint=_compile(
        r"(?:^|/)?query/[^/]+/(?P<qname>[^?\s|]+)",
        re.IGNORECASE,
    ),
    return_result=_compile(
        r"ReturnResult\|\d+\|(?P<ms>\d+)ms\|(?P<engine>[^|]+)\|(?P<rid>[^|]+)\|",
        re.IGNORECASE,
    ),
    req_id=_compile(r"(?P<rid>\d+\.RESTPP_[^,\s|]+)"),
)

GPE = _GpeRegexes(
    start_runudf=_compile(r"\bStart_RunUDF\b"),
    stop_runudf=_compile(r"Stop_RunUDF\|(?P<ms>\d+)\s*ms"),
    udf_step=_compile(
        r'\[UDF_(?P<udf>[^ ]+)\s+log\]\s+"(?P<label>[^"]+)"\s*:\s*(?P<detail>.*)$'
    ),
    iter_in_detail=_compile(r"\biteration:\s*(?P<iter>\d+)\b", re.IGNORECASE),
)

REQUEST_ID = _RequestIdRegexes(
    epoch_ms=_compile(r"\.(?P<epoch_ms>\d{13})(?=\.)"),
)

QUERY_ENDPOINT_RE: re.Pattern[str] = RESTPP.query_endpoint
RETURNRESULT_RE: re.Pattern[str] = RESTPP.return_result

REQ_ID_RE: re.Pattern[str] = RESTPP.req_id

REQID_EPOCH_MS_RE: re.Pattern[str] = REQUEST_ID.epoch_ms
