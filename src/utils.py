import re
from pathlib import Path
import numpy as np
import pandas as pd

RX_YEAR_HEADER = re.compile(r"INFO\.(?P<year>\d{4})(?P<mmdd>\d{4})")
RX_REQ_ID = re.compile(r"(?P<rid>\d+\.RESTPP_[^,\s|]+)")

# RESTPP Specific
RX_QUERY_ENDPOINT = re.compile(
    r"(?:^|/)?query/[^/]+/(?P<qname>[^?\s|]+)", re.IGNORECASE
)
RX_RETURN_RESULT = re.compile(
    r"ReturnResult\|\d+\|(?P<ms>\d+)ms\|(?P<engine>[^|]+)\|(?P<rid>[^|]+)\|",
    re.IGNORECASE,
)

# GPE Specific
RX_UDF_STEP = re.compile(
    r'\[UDF_(?P<udf>[^ ]+)\s+log\]\s+"(?P<label>[^"]+)"\s*:\s*(?P<detail>.*)$'
)
RX_START_RUNUDF = re.compile(r"\bStart_RunUDF\b")
RX_STOP_RUNUDF = re.compile(r"Stop_RunUDF\|(?P<ms>\d+)\s*ms")
RX_ITER_DETAIL = re.compile(r"\biteration:\s*(?P<iter>\d+)\b", re.IGNORECASE)


def detect_year_from_file_header(log_path: Path, default_year: int) -> int:
    """Scans the first 10 lines of a file for a specific INFO header."""
    try:
        with log_path.open("r", errors="replace") as f:
            for i, line in enumerate(f):
                if i > 10:
                    break
                m = RX_YEAR_HEADER.search(line)
                if m:
                    return int(m.group("year"))
    except Exception:
        pass
    return default_year


def pct(series: pd.Series, p: float) -> float:
    """Safe percentile calculation."""
    x = series.dropna().to_numpy()
    return float(np.nanpercentile(x, p)) if len(x) else float("nan")


def extract_request_id(msg: str) -> str | None:
    m = RX_REQ_ID.search(msg)
    return m.group("rid") if m else None
