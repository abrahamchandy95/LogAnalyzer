from dataclasses import dataclass
from typing import NotRequired, TypedDict

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

from common.model.types import QueryName, RequestId, RunId


type Timestampish = pd.Timestamp | NaTType


@dataclass(frozen=True, slots=True)
class RawRequestParsed:
    request_id: RequestId
    method: str | None
    endpoint: str | None
    query_name: QueryName | None


@dataclass(frozen=True, slots=True)
class ReturnResultParsed:
    request_id: RequestId
    ms: float
    engine: str


@dataclass(frozen=True, slots=True)
class RequestInfoParsed:
    request_id: RequestId
    kv: dict[str, str]


@dataclass(frozen=True, slots=True)
class RestppRawRecord:
    parsed: RawRequestParsed


@dataclass(frozen=True, slots=True)
class RestppReturnRecord:
    parsed: ReturnResultParsed


@dataclass(frozen=True, slots=True)
class RestppInfoRecord:
    parsed: RequestInfoParsed


type RestppRecord = RestppRawRecord | RestppReturnRecord | RestppInfoRecord


class RestppRow(TypedDict):
    run: RunId
    node: str
    ts: pd.Timestamp
    tid: int
    request_id: RequestId

    method: NotRequired[str | None]
    endpoint: NotRequired[str | None]
    query_name: NotRequired[QueryName | None]
    graph_name: NotRequired[str | None]

    restpp_return_ms: NotRequired[float]
    restpp_engine: NotRequired[str | None]
    return_ts: NotRequired[Timestampish]

    log_path: str
    lineno: int


OUT_COLS = pd.Index(
    [
        "run",
        "request_id",
        "restpp_ts",
        "restpp_node",
        "endpoint",
        "query_name",
        "graph_name",
        "restpp_return_ms",
        "restpp_engine",
        "restpp_return_ts",
    ]
)
