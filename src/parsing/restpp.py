import numpy as np
from src.parsing.strategies import LogHeader
from src.domain import LogEvent
from src.utils import RX_QUERY_ENDPOINT, RX_RETURN_RESULT


def parse_restpp_event(header: LogHeader, lineno: int) -> LogEvent | None:
    """
    Parses a RESTPP log line into LogEvent
    """
    msg = header.payload
    # ReturnResult
    m_rr = RX_RETURN_RESULT.search(msg)
    if m_rr:
        return LogEvent(
            request_id=m_rr.group("rid").strip(),
            event="ReturnResult",
            duration_ms=int(m_rr.group("ms")),
            engine=m_rr.group("engine"),
            msg=msg,
            lineno=lineno,
        )
    if "RawRequest|," in msg:
        try:
            after = msg.split("RawRequest|,", 1)[1]
            request_id, rest = after.split(",", 1)
            parts = rest.split("|")
            endpoint = parts[2] if len(parts) > 2 else ""

            qname = None
            if endpoint:
                m_q = RX_QUERY_ENDPOINT.search(endpoint)
                if m_q:
                    qname = m_q.group("qname")

            return LogEvent(
                request_id=request_id.strip(),
                event="RawRequest",
                endpoint=endpoint,
                query_name=qname,
                duration_ms=np.nan,
                msg=msg,
                lineno=lineno,
            )
        except (IndexError, ValueError):
            return None
    return None
