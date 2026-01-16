from dataclasses import dataclass

from common.parse.regexes import GPE
from common.parse.request_id import extract_request_id

from .records import (
    StepParsed,
    UdfStartParsed,
    UdfStopParsed,
    GpeRecord,
    GpeStepRecord,
    GpeUdfStartRecord,
    GpeUdfStopRecord,
)


@dataclass(frozen=True, slots=True)
class DecodedGpe:
    record: GpeRecord
    request_id: str | None


def _parse_step(msg: str) -> StepParsed | None:
    m_step = GPE.udf_step.search(msg)
    if not m_step:
        return None

    detail = m_step.group("detail")
    m_iter = GPE.iter_in_detail.search(detail)
    iter_no = int(m_iter.group("iter")) if m_iter else None

    return StepParsed(
        udf=m_step.group("udf"),
        label=m_step.group("label"),
        detail=detail,
        iteration=iter_no,
    )


def _parse_udf_start(msg: str) -> UdfStartParsed | None:
    if GPE.start_runudf.search(msg):
        return UdfStartParsed(detail=msg)
    return None


def _parse_udf_stop(msg: str) -> UdfStopParsed | None:
    m_stop = GPE.stop_runudf.search(msg)
    if not m_stop:
        return None
    return UdfStopParsed(detail=msg, ms=float(m_stop.group("ms")))


def _classify_record(msg: str) -> GpeRecord | None:
    step = _parse_step(msg)
    if step is not None:
        return GpeStepRecord(parsed=step)

    start = _parse_udf_start(msg)
    if start is not None:
        return GpeUdfStartRecord(parsed=start)

    stop = _parse_udf_stop(msg)
    if stop is not None:
        return GpeUdfStopRecord(parsed=stop)

    return None


def decode_msg(msg: str) -> DecodedGpe | None:
    """
    Pure decode:
      msg -> (record + request_id)
    No ParsedLine, no pandas, no filesystem concerns.
    """
    rec = _classify_record(msg)
    if rec is None:
        return None

    rid = extract_request_id(msg)
    return DecodedGpe(record=rec, request_id=rid)
