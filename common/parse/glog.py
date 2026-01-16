from pathlib import Path
from datetime import datetime

from common.parse.regexes import LOG_HEADER_DATE_RE, GLOG_INFO_LINE_RE
from common.model.types import GlogEntry


def detect_year_from_header(log_path: Path, default_year: int) -> int:
    """
    Extract year from a log header line like INFO.20251219..., if present.
    """
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
    """
    Parse a glog INFO line prefix and return (ts, tid, msg).
    """
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
