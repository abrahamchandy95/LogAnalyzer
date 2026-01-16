from math import nan

from common.model.constants import GPE_STEP, GPE_UDF_START, GPE_UDF_STOP
from common.parse.regexes import (
    ITER_IN_DETAIL_RE,
    START_RUNUDF_RE,
    STOP_RUNUDF_RE,
    UDF_STEP_RE,
)
from common.parse.request_id import extract_request_id
from parsers._walker import ParsedLine

from .records import (
    GpeRow,
    StepParsed,
    UdfStartParsed,
    UdfStopParsed,
    GpeRecord,
    GpeStepRecord,
    GpeUdfStartRecord,
    GpeUdfStopRecord,
)


def parse_step(msg: str) -> StepParsed | None:
    m_step = UDF_STEP_RE.search(msg)
    if not m_step:
        return None

    detail = m_step.group("detail")
    m_iter = ITER_IN_DETAIL_RE.search(detail)
    iter_no = int(m_iter.group("iter")) if m_iter else None

    return StepParsed(
        udf=m_step.group("udf"),
        label=m_step.group("label"),
        detail=detail,
        iteration=iter_no,
    )


def parse_udf_start(msg: str) -> UdfStartParsed | None:
    if START_RUNUDF_RE.search(msg):
        return UdfStartParsed(detail=msg)
    return None


def parse_udf_stop(msg: str) -> UdfStopParsed | None:
    m_stop = STOP_RUNUDF_RE.search(msg)
    if not m_stop:
        return None
    return UdfStopParsed(detail=msg, ms=float(m_stop.group("ms")))


def classify_msg(msg: str) -> GpeRecord | None:
    step = parse_step(msg)
    if step is not None:
        return GpeStepRecord(parsed=step)

    start = parse_udf_start(msg)
    if start is not None:
        return GpeUdfStartRecord(parsed=start)

    stop = parse_udf_stop(msg)
    if stop is not None:
        return GpeUdfStopRecord(parsed=stop)

    return None


def base_row(
    *,
    pl: ParsedLine,
    request_id: str | None,
    event: str,
    label: str,
    detail: str,
    udf_ms: float,
) -> GpeRow:
    return {
        "run": pl.run,
        "node": pl.node,
        "ts": pl.ts,
        "tid": pl.tid,
        "request_id": request_id,
        "event": event,
        "label": label,
        "detail": detail,
        "udf_ms": udf_ms,
        "log_path": str(pl.log_path),
        "lineno": pl.lineno,
        "raw_msg": pl.msg,
    }


def make_step_row(
    *, pl: ParsedLine, request_id: str | None, parsed: StepParsed
) -> GpeRow:
    row = base_row(
        pl=pl,
        request_id=request_id,
        event=GPE_STEP,
        label=parsed.label,
        detail=parsed.detail,
        udf_ms=nan,
    )
    row["udf"] = parsed.udf
    row["iteration"] = parsed.iteration
    return row


def make_udf_start_row(
    *, pl: ParsedLine, request_id: str | None, parsed: UdfStartParsed
) -> GpeRow:
    row = base_row(
        pl=pl,
        request_id=request_id,
        event=GPE_UDF_START,
        label=GPE_UDF_START,
        detail=parsed.detail,
        udf_ms=nan,
    )
    row["udf"] = None
    row["iteration"] = None
    return row


def make_udf_stop_row(
    *, pl: ParsedLine, request_id: str | None, parsed: UdfStopParsed
) -> GpeRow:
    row = base_row(
        pl=pl,
        request_id=request_id,
        event=GPE_UDF_STOP,
        label=GPE_UDF_STOP,
        detail=parsed.detail,
        udf_ms=parsed.ms,
    )
    row["udf"] = None
    row["iteration"] = None
    return row


def extract_rid(msg: str) -> str | None:
    # shared logic; returns str|None; RequestId is an alias of str anyway
    return extract_request_id(msg)
