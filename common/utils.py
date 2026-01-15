from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime

from common.regexes import LOG_HEADER_DATE_RE, GLOG_INFO_LINE_RE, REQ_ID_RE
from common.types import GlogEntry


def detect_year_from_header(log_path: Path, default_year: int) -> int:
    try:
        with log_path.open("r", errors="replace") as f:
            for i, line in enumerate(f):
                if i > 10:
                    break
                m = LOG_HEADER_DATE_RE.search(line)
                if m:
                    return int(m.group("year"))
    except Exception:
        pass
    return default_year


def parse_glog_line(line: str, year: int) -> GlogEntry | None:
    m = GLOG_INFO_LINE_RE.match(line)
    if not m:
        return None

    try:
        ts = datetime.strptime(
            f"{year}{m.group('mm')}{m.group('dd')} {m.group('hms')}",
            "%Y%m%d %H:%M:%S.%f",
        )
    except ValueError:
        return None

    return GlogEntry(ts=ts, tid=int(m.group("tid")), msg=m.group("msg"))


def extract_request_id(msg: str) -> str | None:
    m = REQ_ID_RE.search(msg)
    return m.group("rid") if m else None


def pct(s: pd.Series, p: float) -> float:
    x = s.dropna().to_numpy()
    return float(np.nanpercentile(x, p)) if len(x) else float("nan")
