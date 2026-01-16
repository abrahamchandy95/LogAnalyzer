from math import nan

from common.model.constants import GPE_STEP, GPE_UDF_START, GPE_UDF_STOP
from parsers._walker import ParsedLine

from .decode import DecodedGpe
from .records import GpeRow, GpeStepRecord, GpeUdfStartRecord, GpeUdfStopRecord


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


def row_from_decoded(pl: ParsedLine, dec: DecodedGpe) -> GpeRow:
    """
    SRP: map (ParsedLine + decoded record) -> a single GPE row.
    No pandas, no dedupe, no dataframe building.
    """
    rid = dec.request_id
    rec = dec.record

    match rec:
        case GpeStepRecord(parsed=step):
            row = base_row(
                pl=pl,
                request_id=rid,
                event=GPE_STEP,
                label=step.label,
                detail=step.detail,
                udf_ms=nan,
            )
            row["udf"] = step.udf
            row["iteration"] = step.iteration
            return row

        case GpeUdfStartRecord(parsed=start):
            row = base_row(
                pl=pl,
                request_id=rid,
                event=GPE_UDF_START,
                label=GPE_UDF_START,
                detail=start.detail,
                udf_ms=nan,
            )
            row["udf"] = None
            row["iteration"] = None
            return row

        case GpeUdfStopRecord(parsed=stop):
            row = base_row(
                pl=pl,
                request_id=rid,
                event=GPE_UDF_STOP,
                label=GPE_UDF_STOP,
                detail=stop.detail,
                udf_ms=stop.ms,
            )
            row["udf"] = None
            row["iteration"] = None
            return row

    # Runtime safety fallback (should be unreachable)
    row = base_row(
        pl=pl,
        request_id=rid,
        event="UNKNOWN",
        label="UNKNOWN",
        detail=pl.msg,
        udf_ms=nan,
    )
    row["udf"] = None
    row["iteration"] = None
    return row
