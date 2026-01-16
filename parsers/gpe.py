from math import nan
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, NotRequired

import pandas as pd

from common.parse.regexes import (
    ITER_IN_DETAIL_RE,
    REQ_ID_RE,
    START_RUNUDF_RE,
    STOP_RUNUDF_RE,
    UDF_STEP_RE,
)
from common.model.types import Node, RequestId, RunId
from common.support.reporting import Reporter, NullReporter
from common.model.constants import GPE_GLOB, GPE_STEP, GPE_UDF_START, GPE_UDF_STOP
from parsers._walker import ParsedLine, walk_logs


class _GpeRow(TypedDict):
    run: RunId
    node: Node
    ts: pd.Timestamp
    tid: int
    request_id: NotRequired[RequestId | None]
    event: str
    udf: NotRequired[str | None]
    label: str
    iteration: NotRequired[int | None]
    detail: str
    udf_ms: float
    log_path: str
    lineno: int
    raw_msg: str


_OUT_COLS = pd.Index(
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

_GPE_DEDUPE_SUBSET: list[str] = ["run", "node", "tid", "ts", "raw_msg"]


def _dedupe_gpe_events(df: pd.DataFrame, *, reporter: Reporter) -> pd.DataFrame:
    """
    Drop duplicate GPE events that are repeated across different files (log rotation / bundles).
    We intentionally do NOT include log_path/lineno in the key because duplicates differ there.
    """
    if df.empty:
        return df

    # stable ordering so keep="first" is deterministic
    df2 = df.sort_values(
        ["run", "node", "tid", "ts", "log_path", "lineno"],
        kind="mergesort",
        na_position="last",
    )

    before = len(df2)
    df2 = df2.drop_duplicates(subset=_GPE_DEDUPE_SUBSET, keep="first")
    after = len(df2)

    dropped = before - after
    if dropped:
        reporter.info(
            f"GPE dedupe: dropped {dropped} duplicate events ({before} -> {after})"
        )

    return df2


@dataclass(frozen=True, slots=True)
class StepParsed:
    udf: str
    label: str
    detail: str
    iteration: int | None


@dataclass(frozen=True, slots=True)
class UdfStartParsed:
    detail: str


@dataclass(frozen=True, slots=True)
class UdfStopParsed:
    detail: str
    ms: float


@dataclass(frozen=True, slots=True)
class GpeStepRecord:
    parsed: StepParsed


@dataclass(frozen=True, slots=True)
class GpeUdfStartRecord:
    parsed: UdfStartParsed


@dataclass(frozen=True, slots=True)
class GpeUdfStopRecord:
    parsed: UdfStopParsed


type GpeRecord = GpeStepRecord | GpeUdfStartRecord | GpeUdfStopRecord


def _extract_request_id(msg: str) -> RequestId | None:
    m = REQ_ID_RE.search(msg)
    return m.group("rid") if m else None


def _parse_step(msg: str) -> StepParsed | None:
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


def _parse_udf_start(msg: str) -> UdfStartParsed | None:
    if START_RUNUDF_RE.search(msg):
        return UdfStartParsed(detail=msg)
    return None


def _parse_udf_stop(msg: str) -> UdfStopParsed | None:
    m_stop = STOP_RUNUDF_RE.search(msg)
    if not m_stop:
        return None
    return UdfStopParsed(detail=msg, ms=float(m_stop.group("ms")))


def _classify_msg(msg: str) -> GpeRecord | None:
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


def _base_row(
    *,
    pl: ParsedLine,
    request_id: RequestId | None,
    event: str,
    label: str,
    detail: str,
    udf_ms: float,
) -> _GpeRow:
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


def _make_step_row(
    *, pl: ParsedLine, request_id: RequestId | None, parsed: StepParsed
) -> _GpeRow:
    row = _base_row(
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


def _make_udf_start_row(
    *, pl: ParsedLine, request_id: RequestId | None, parsed: UdfStartParsed
) -> _GpeRow:
    row = _base_row(
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


def _make_udf_stop_row(
    *, pl: ParsedLine, request_id: RequestId | None, parsed: UdfStopParsed
) -> _GpeRow:
    row = _base_row(
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


def parse_gpe(
    run_key: RunId,
    run_dir: Path,
    *,
    nodes: tuple[Node, ...],
    reporter: Reporter | None = None,
) -> pd.DataFrame:
    rep: Reporter = reporter if reporter is not None else NullReporter()
    rows: list[_GpeRow] = []

    def _on_line(pl: ParsedLine) -> None:
        rec = _classify_msg(pl.msg)
        if rec is None:
            return

        rid = _extract_request_id(pl.msg)

        match rec:
            case GpeStepRecord(parsed=step):
                rows.append(_make_step_row(pl=pl, request_id=rid, parsed=step))
            case GpeUdfStartRecord(parsed=start):
                rows.append(_make_udf_start_row(pl=pl, request_id=rid, parsed=start))
            case GpeUdfStopRecord(parsed=stop):
                rows.append(_make_udf_stop_row(pl=pl, request_id=rid, parsed=stop))

    walk_logs(
        run_id=run_key,
        run_dir=run_dir,
        nodes=nodes,
        file_glob=GPE_GLOB,
        on_line=_on_line,
    )

    if not rows:
        return pd.DataFrame(columns=_OUT_COLS)

    df = pd.DataFrame(rows).reindex(columns=_OUT_COLS)

    df = _dedupe_gpe_events(df, reporter=rep)

    df = df.set_index(["run", "node", "tid", "ts"]).sort_index().reset_index()
    return df.reset_index(drop=True)
