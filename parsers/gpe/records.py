from dataclasses import dataclass
from typing import NotRequired, TypedDict

import pandas as pd

from common.model.types import Node, RequestId, RunId


class GpeRow(TypedDict):
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


OUT_COLS = pd.Index(
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

# Dedupe key: duplicated across files differ only by (log_path, lineno)
GPE_DEDUPE_SUBSET: list[str] = ["run", "node", "tid", "ts", "raw_msg"]


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
