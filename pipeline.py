from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from common.config import CompareConfig
from common.types import RunInput

from parsers.gpe import parse_gpe
from parsers.restpp import parse_restpp
from transforms.attach import attach_steps_to_requests
from transforms.gaps import add_query_name, build_gaps
from analysis.stats import (
    build_ordered_step_side_table,
    compare_two_queries,
    make_step_stats,
)
from analysis.bottlenecks import top_bottlenecks
from analysis.requests import build_exec_request_table, extract_ids, summarize_requests
from export.writers import write_csv, write_lines
from export.plot import plot_step_means


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
    """The final statists comparing the two query variants."""

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


def _ingest_logs(runs: tuple[RunInput, ...], nodes: tuple[str, ...]) -> LogExtracts:
    """
    Reads RESTPP and GPE logs from disk and consolidates them.
    """
    rest_frames: list[pd.DataFrame] = []
    gpe_frames: list[pd.DataFrame] = []

    for run in runs:
        if not run.path.exists():
            raise FileNotFoundError(f"Run directory not found: {run.path}")

        rest_frames.append(parse_restpp(run.key, run.path, nodes=nodes))
        gpe_frames.append(parse_gpe(run.key, run.path, nodes=nodes))

    requests = (
        pd.concat(rest_frames, ignore_index=True) if rest_frames else pd.DataFrame()
    )
    events = pd.concat(gpe_frames, ignore_index=True) if gpe_frames else pd.DataFrame()

    return LogExtracts(rest_requests=requests, gpe_events=events)


def _process_events(logs: LogExtracts) -> QueryEvents:
    """
    Links distributed events to requests and calculates step durations.
    """
    # Link distributed GPE events back to the initiating REST request
    linked = attach_steps_to_requests(logs.gpe_events)

    # Calculate the time gaps (latency) between processing steps
    raw_gaps = build_gaps(linked)

    # Tag these durations with the specific query name (Base vs Opt)
    timings = add_query_name(raw_gaps, logs.rest_requests)

    return QueryEvents(linked_events=linked, step_timings=timings)


def _compare_performance(
    logs: LogExtracts, events: QueryEvents, base_query: str, opt_query: str
) -> PerformanceComparison:
    """
    Generates statistical comparisons between the Base and Optimized queries.
    """
    req_summary = summarize_requests(logs.rest_requests, events.linked_events)
    exec_table = build_exec_request_table(logs.rest_requests, events.linked_events)

    base_ids, opt_ids = extract_ids(exec_table, base_query, opt_query)

    step_stats = make_step_stats(events.step_timings)

    q_vs_q = compare_two_queries(step_stats, base_query, opt_query)

    side_by_side = build_ordered_step_side_table(
        events.step_timings,
        base_query=base_query,
        opt_query=opt_query,
        step_prefix="Step ",
    )

    bott_base = top_bottlenecks(events.step_timings, base_query, n=50)
    bott_opt = top_bottlenecks(events.step_timings, opt_query, n=50)

    return PerformanceComparison(
        request_summary=req_summary,
        execution_table=exec_table,
        step_statistics=step_stats,
        query_vs_query_stats=q_vs_q,
        step_side_by_side=side_by_side,
        bottlenecks_base=bott_base,
        bottlenecks_opt=bott_opt,
        base_request_ids=base_ids,
        opt_request_ids=opt_ids,
    )


def execute_pipeline(cfg: CompareConfig) -> PipelineOutput:
    """
    Orchestrates the log analysis pipeline: Ingest -> Process -> Compare.
    """
    print("1. Ingesting logs...")
    extracts = _ingest_logs(cfg.runs, cfg.nodes)

    print("2. Processing query events...")
    events = _process_events(extracts)

    print("3. Comparing performance...")
    comparison = _compare_performance(extracts, events, cfg.base_query, cfg.opt_query)

    return PipelineOutput(extracts=extracts, events=events, comparison=comparison)


def save_artifacts(results: PipelineOutput, out_dir: Path) -> Path | None:
    """
    Writes all analysis artifacts (CSVs, IDs, Plots) to the output directory.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Writing outputs to: {out_dir}")

    comp = results.comparison
    ev = results.events
    ex = results.extracts

    write_csv(ex.rest_requests, out_dir / "restpp_requests.csv")
    write_csv(ev.linked_events, out_dir / "gpe_events_attached.csv")
    write_csv(ev.step_timings, out_dir / "gaps_with_query.csv")

    write_csv(comp.request_summary, out_dir / "request_summary.csv")
    write_csv(comp.execution_table, out_dir / "exec_request_table.csv")
    write_csv(comp.step_statistics, out_dir / "step_stats.csv")
    write_csv(comp.query_vs_query_stats, out_dir / "compare_two_queries.csv")
    write_csv(comp.step_side_by_side, out_dir / "side_ordered_steps.csv")

    write_csv(comp.bottlenecks_base, out_dir / "bottlenecks_base.csv")
    write_csv(comp.bottlenecks_opt, out_dir / "bottlenecks_opt.csv")
    write_lines(comp.base_request_ids, out_dir / "base_request_ids.txt")
    write_lines(comp.opt_request_ids, out_dir / "opt_request_ids.txt")

    plot_path = None
    side = comp.step_side_by_side

    if (
        not side.empty
        and "base_mean_ms" in side.columns
        and "opt_mean_ms" in side.columns
    ):
        plot_path = out_dir / "step_means_base_vs_opt.png"
        plot_step_means(
            side, out_path=plot_path, title="Per-step mean duration: Base vs Optimized"
        )

    return plot_path
