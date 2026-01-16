import pandas as pd

from analysis.bottlenecks import top_bottlenecks
from analysis.requests import build_exec_request_table, extract_ids, summarize_requests
from analysis.step_stats import (
    build_ordered_step_side_table,
    compare_two_queries,
    make_step_stats,
)
from common.model.config import CompareConfig
from common.support.reporting import NullReporter, Reporter
from common.model.results import (
    LogExtracts,
    PerformanceComparison,
    PipelineOutput,
    QueryEvents,
)
from common.model.types import RunInput
from parsers.gpe import parse_gpe
from parsers.restpp import parse_restpp
from transforms.attach import attach_steps_to_requests
from transforms.gaps import add_query_name, build_gaps


def _ingest_logs(runs: tuple[RunInput, ...], nodes: tuple[str, ...]) -> LogExtracts:
    rest_frames: list[pd.DataFrame] = []
    gpe_frames: list[pd.DataFrame] = []

    for run in runs:
        if not run.path.exists():
            raise FileNotFoundError(f"Run directory not found: {run.path}")

        rest_frames.append(parse_restpp(run.id, run.path, nodes=nodes))
        gpe_frames.append(parse_gpe(run.id, run.path, nodes=nodes))

    requests = (
        pd.concat(rest_frames, ignore_index=True) if rest_frames else pd.DataFrame()
    )
    events = pd.concat(gpe_frames, ignore_index=True) if gpe_frames else pd.DataFrame()

    return LogExtracts(rest_requests=requests, gpe_events=events)


def _process_events(logs: LogExtracts) -> QueryEvents:
    linked = attach_steps_to_requests(logs.gpe_events)
    raw_gaps = build_gaps(linked)
    timings = add_query_name(raw_gaps, logs.rest_requests)
    return QueryEvents(linked_events=linked, step_timings=timings)


def _compare_performance(
    logs: LogExtracts, events: QueryEvents, base_query: str, opt_query: str
) -> PerformanceComparison:
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


def run_performance_analysis(
    cfg: CompareConfig, *, reporter: Reporter | None = None
) -> PipelineOutput:
    """
    Orchestrates the log analysis pipeline.
    """
    rep: Reporter = reporter if reporter is not None else NullReporter()

    rep.info("1. Ingesting logs...")
    extracts = _ingest_logs(cfg.runs, cfg.nodes)

    rep.info("2. Processing query events...")
    events = _process_events(extracts)

    rep.info("3. Comparing performance...")
    comparison = _compare_performance(extracts, events, cfg.base_query, cfg.opt_query)

    return PipelineOutput(extracts=extracts, events=events, comparison=comparison)
