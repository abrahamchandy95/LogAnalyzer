from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from common.parse.glog import detect_year_from_header
from common.parse.request_id import extract_epoch_ms_from_request_id, extract_request_id


@dataclass(frozen=True, slots=True)
class _FileSig:
    """
    Cheap fingerprint to avoid re-scanning the same file across the pipeline.
    If the file is rewritten/rotated, mtime/size will typically change.
    """

    path: str
    mtime_ns: int
    size: int


def _sig(p: Path) -> _FileSig:
    st = p.stat()
    return _FileSig(path=str(p.resolve()), mtime_ns=st.st_mtime_ns, size=st.st_size)


@lru_cache(maxsize=2048)
def _infer_year_cached(sig: _FileSig, default_year: int, max_lines: int) -> int:
    p = Path(sig.path)

    # First: try epoch from request id in early lines
    try:
        with p.open("r", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break

                rid = extract_request_id(line)
                if not rid:
                    continue

                epoch_ms = extract_epoch_ms_from_request_id(rid)
                if epoch_ms is None:
                    continue

                return datetime.fromtimestamp(epoch_ms / 1000.0).year
    except Exception:
        # fall back below
        pass

    # Second: try header year
    return detect_year_from_header(p, default_year=default_year)


def infer_year_from_any_line_epoch(
    log_path: Path,
    default_year: int,
    *,
    max_lines: int = 2000,
) -> int:
    """
    Infer year by scanning for epoch-ms embedded in a request id.
    Uses caching so each log file is scanned at most once per pipeline run.
    Falls back to header year, then default_year.
    """
    try:
        sig = _sig(log_path)
    except Exception:
        # If stat fails, just do the fallback behavior without cache.
        return detect_year_from_header(log_path, default_year=default_year)

    return _infer_year_cached(sig, default_year, max_lines)
