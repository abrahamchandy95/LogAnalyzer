import numpy as np

from src.parsing.strategies import LogHeader
from src.domain import LogEvent
from src.utils import (
    RX_UDF_STEP,
    RX_ITER_DETAIL,
    RX_START_RUNUDF,
    RX_STOP_RUNUDF,
    extract_request_id,
)


def parse_gpe_event(header: LogHeader, lineno: int) -> LogEvent | None:
    """
    Parses a GPE log line into LogEvent
    """
    msg = header.payload
    rid = extract_request_id(msg)
    # step event
    m_step = RX_UDF_STEP.search(msg)
    if m_step:
        detail = m_step.group("detail")
        m_iter = RX_ITER_DETAIL.search(detail)
        iter_no = int(m_iter.group("iter")) if m_iter else None

        return LogEvent(
            request_id=rid,
            event="STEP",
            label=m_step.group("label"),
            duration_ms=np.nan,
            detail=detail,
            iteration=iter_no,
            msg=msg,
            lineno=lineno,
        )

    # UDF start
    if RX_START_RUNUDF.search(msg):
        return LogEvent(
            request_id=rid,
            event="UDF_START",
            label="UDF_START",
            duration_ms=np.nan,
            msg=msg,
            lineno=lineno,
        )

    # UDF stop
    m_stop = RX_STOP_RUNUDF.search(msg)
    if m_stop:
        return LogEvent(
            request_id=rid,
            event="UDF_STOP",
            label="UDF_STOP",
            duration_ms=int(m_stop.group("ms")),
            msg=msg,
            lineno=lineno,
        )
    return None
