from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True, slots=True)
class LogExtracts:
    """The raw dataframes ingested directly from the log files."""

    rest_requests: pd.DataFrame
    gpe_events: pd.DataFrame


@dataclass(frozen=True, slots=True)
class QueryEvents:
    """Events processed and linked to query executions."""

    linked_events: pd.DataFrame
    step_timings: pd.DataFrame


@dataclass(frozen=True, slots=True)
class PerformanceComparison:
    """The final statistical artifacts comparing the two query variants."""

    # Summaries
    request_summary: pd.DataFrame
    execution_table: pd.DataFrame

    # Statistical Analysis
    step_statistics: pd.DataFrame
    query_vs_query_stats: pd.DataFrame
    step_side_by_side: pd.DataFrame

    # Bottleneck Analysis
    bottlenecks_base: pd.DataFrame
    bottlenecks_opt: pd.DataFrame

    # Traceability
    base_request_ids: list[str]
    opt_request_ids: list[str]


@dataclass(frozen=True, slots=True)
class PipelineOutput:
    """Container for all artifacts produced during the run."""

    extracts: LogExtracts
    events: QueryEvents
    comparison: PerformanceComparison
