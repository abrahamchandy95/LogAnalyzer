from math import nan
from pathlib import Path
from typing import TypedDict, TypeIs

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

from common.regexes import (
    ITER_IN_DETAIL_RE,
    REQ_ID_RE,
    START_RUNUDF_RE,
    STOP_RUNUDF_RE,
    UDF_STEP_RE,
)
from common.utils import detect_year_from_header, parse_glog_line


type Timestampish = pd.Timestamp | NaTType


def is_real_timestamp(x: Timestampish) -> TypeIs[pd.Timestamp]:
    # Pandas typing: pd.Timestamp(...) can be Timestamp | NaTType
    return not isinstance(x, NaTType)


class _GpeRow(TypedDict):
    run: str
    node: str
    ts: pd.Timestamp
    tid: int
    request_id: str | None
    event: str
    udf: str | None
    label: str
    iteration: int | None
    detail: str
    udf_ms: float
    log_path: str
    lineno: int
    raw_msg: str


EMPTY_GPE_COLS = pd.Index(
    [
        "run",
        "node",
        "ts",
        "tid",
        "request_id",
        "event",
        "udf",
        "label",
        "iteration",
        "detail",
        "udf_ms",
        "log_path",
        "lineno",
        "raw_msg",
    ]
)


def parse_gpe(run_key: str, run_dir: Path, *, nodes: tuple[str, ...]) -> pd.DataFrame:
    rows: list[_GpeRow] = []
    folder_year = int(run_key.split("-", 1)[0]) if "-" in run_key else 2026

    for node in nodes:
        node_dir = run_dir / node
        if not node_dir.exists():
            continue

        for log_path in node_dir.glob("gpe*"):
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

                    rid_match = REQ_ID_RE.search(msg)
                    rid = rid_match.group("rid") if rid_match else None

                    m_step = UDF_STEP_RE.search(msg)
                    if m_step:
                        detail = m_step.group("detail")
                        m_iter = ITER_IN_DETAIL_RE.search(detail)
                        iter_no = int(m_iter.group("iter")) if m_iter else None

                        step_row: _GpeRow = {
                            "run": run_key,
                            "node": node,
                            "ts": ts,
                            "tid": tid,
                            "request_id": rid,
                            "event": "STEP",
                            "udf": m_step.group("udf"),
                            "label": m_step.group("label"),
                            "iteration": iter_no,
                            "detail": detail,
                            "udf_ms": nan,
                            "log_path": str(log_path),
                            "lineno": lineno,
                            "raw_msg": msg,
                        }
                        rows.append(step_row)
                        continue

                    if START_RUNUDF_RE.search(msg):
                        start_row: _GpeRow = {
                            "run": run_key,
                            "node": node,
                            "ts": ts,
                            "tid": tid,
                            "request_id": rid,
                            "event": "UDF_START",
                            "udf": None,
                            "label": "UDF_START",
                            "iteration": None,
                            "detail": msg,
                            "udf_ms": nan,
                            "log_path": str(log_path),
                            "lineno": lineno,
                            "raw_msg": msg,
                        }
                        rows.append(start_row)
                        continue

                    m_stop = STOP_RUNUDF_RE.search(msg)
                    if m_stop:
                        stop_row: _GpeRow = {
                            "run": run_key,
                            "node": node,
                            "ts": ts,
                            "tid": tid,
                            "request_id": rid,
                            "event": "UDF_STOP",
                            "udf": None,
                            "label": "UDF_STOP",
                            "iteration": None,
                            "detail": msg,
                            "udf_ms": float(m_stop.group("ms")),
                            "log_path": str(log_path),
                            "lineno": lineno,
                            "raw_msg": msg,
                        }
                        rows.append(stop_row)

    if not rows:
        return pd.DataFrame(columns=EMPTY_GPE_COLS)

    df = pd.DataFrame(rows)

    # Typed-friendly multi-key sort: avoid pandas sort_values(list[str]) overload issues
    df = df.set_index(["run", "node", "tid", "ts"]).sort_index().reset_index()

    return df.reset_index(drop=True)
