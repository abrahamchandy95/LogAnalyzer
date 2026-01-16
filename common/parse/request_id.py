from common.parse.regexes import REQ_ID_RE, REQUEST_ID


def extract_request_id(msg: str) -> str | None:
    """
    Extract the TigerGraph RESTPP request id from a log message.
    """
    m = REQ_ID_RE.search(msg)
    return m.group("rid") if m else None


def extract_epoch_ms_from_request_id(rid: str) -> int | None:
    """
    Extract epoch-milliseconds from a RESTPP request id string, e.g.
      16974725.RESTPP_1_1.1766154007634.N  -> 1766154007634
    """
    m = REQUEST_ID.epoch_ms.search(rid)
    if not m:
        return None
    try:
        return int(m.group("epoch_ms"))
    except ValueError:
        return None
