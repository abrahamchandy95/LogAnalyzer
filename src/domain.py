from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import pandas as pd
from src.types import QueryName, ExecutionID


@dataclass(frozen=True)
class LogSource:
    """
    Identifies origin of the log file. Use to trace log line back to server
    and execution batch.
    """

    execution_id: ExecutionID
    server_node: str
    file_path: Path


@dataclass
class BenchmarkReport:
    """
    Holds the stats of the two query variants
    """

    base_variant: QueryName
    candidate_variant: QueryName

    # compare step durations
    step_latency_comparison: pd.DataFrame

    # rows of slowest occurrences
    base_bottlenecks: pd.DataFrame
    candidate_bottlenecks: pd.DataFrame


class LogEvent(TypedDict, total=False):
    """
    Standardizes the output of all LineProcessors
    """

    # Crawler metadata
    run: str
    node: str
    ts: pd.Timestamp
    tid: int
    log_path: str

    request_id: str | None
    event: str
    label: str | None
    duration_ms: float
    msg: str
    lineno: int

    # GPE specific
    detail: str | None
    iteration: int | None

    # RESTPP specific
    endpoint: str | None
    query_name: str | None
    engine: str | None
