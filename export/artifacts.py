from pathlib import Path

from common.support.reporting import NullReporter, Reporter
from common.model.results import PipelineOutput
from export.paths import OutputPaths, build_output_paths
from export.plot import plot_step_means
from export.writers import write_csv, write_lines


def save_all_artifacts(
    results: PipelineOutput,
    out_dir: Path,
    *,
    reporter: Reporter | None = None,
) -> Path | None:
    """
    Persist all analysis artifacts.
    Returns the path to the main plot if it was generated.
    """
    rep: Reporter = reporter if reporter is not None else NullReporter()

    paths = build_output_paths(out_dir)
    paths.out_dir.mkdir(parents=True, exist_ok=True)

    rep.info(f"Writing outputs to: {paths.out_dir}")

    _write_tables(results, paths)
    _write_traceability(results, paths)
    plot_path = _write_plot(results, paths)

    return plot_path


def _write_tables(results: PipelineOutput, paths: OutputPaths) -> None:
    ex = results.extracts
    ev = results.events
    cmp = results.comparison

    write_csv(ex.rest_requests, paths.restpp_requests_csv)
    write_csv(ev.linked_events, paths.gpe_events_attached_csv)
    write_csv(ev.step_timings, paths.gaps_with_query_csv)

    write_csv(cmp.request_summary, paths.request_summary_csv)
    write_csv(cmp.execution_table, paths.exec_request_table_csv)
    write_csv(cmp.step_statistics, paths.step_stats_csv)
    write_csv(cmp.query_vs_query_stats, paths.compare_two_queries_csv)
    write_csv(cmp.step_side_by_side, paths.side_ordered_steps_csv)

    write_csv(cmp.bottlenecks_base, paths.bottlenecks_base_csv)
    write_csv(cmp.bottlenecks_opt, paths.bottlenecks_opt_csv)


def _write_traceability(results: PipelineOutput, paths: OutputPaths) -> None:
    cmp = results.comparison
    write_lines(cmp.base_request_ids, paths.base_request_ids_txt)
    write_lines(cmp.opt_request_ids, paths.opt_request_ids_txt)


def _write_plot(results: PipelineOutput, paths: OutputPaths) -> Path | None:
    side = results.comparison.step_side_by_side

    if (
        side.empty
        or "base_mean_ms" not in side.columns
        or "opt_mean_ms" not in side.columns
    ):
        return None

    plot_step_means(
        side,
        out_path=paths.step_means_png,
        title="Per-step mean duration: Base vs Optimized",
    )
    return paths.step_means_png
