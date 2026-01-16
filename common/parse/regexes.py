import re

GLOG_INFO_LINE_RE: re.Pattern[str] = re.compile(
    r"^I(?P<mm>\d{2})(?P<dd>\d{2})\s+(?P<hms>\d{2}:\d{2}:\d{2}\.\d+)\s+(?P<tid>\d+)\s+.*?\]\s+(?P<msg>.*)$"
)
LOG_HEADER_DATE_RE: re.Pattern[str] = re.compile(
    r"INFO\.(?P<year>\d{4})(?P<mmdd>\d{4})"
)

QUERY_ENDPOINT_RE: re.Pattern[str] = re.compile(
    r"(?:^|/)?query/[^/]+/(?P<qname>[^?\s|]+)", re.IGNORECASE
)
RETURNRESULT_RE: re.Pattern[str] = re.compile(
    r"ReturnResult\|\d+\|(?P<ms>\d+)ms\|(?P<engine>[^|]+)\|(?P<rid>[^|]+)\|",
    re.IGNORECASE,
)

REQ_ID_RE: re.Pattern[str] = re.compile(r"(?P<rid>\d+\.RESTPP_[^,\s|]+)")
START_RUNUDF_RE: re.Pattern[str] = re.compile(r"\bStart_RunUDF\b")
STOP_RUNUDF_RE: re.Pattern[str] = re.compile(r"Stop_RunUDF\|(?P<ms>\d+)\s*ms")

UDF_STEP_RE: re.Pattern[str] = re.compile(
    r'\[UDF_(?P<udf>[^ ]+)\s+log\]\s+"(?P<label>[^"]+)"\s*:\s*(?P<detail>.*)$'
)
ITER_IN_DETAIL_RE: re.Pattern[str] = re.compile(
    r"\biteration:\s*(?P<iter>\d+)\b", re.IGNORECASE
)

REQID_EPOCH_MS_RE: re.Pattern[str] = re.compile(r"\.(?P<epoch_ms>\d{13})(?=\.)")
