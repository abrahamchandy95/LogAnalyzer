from pathlib import Path

from common.results import PipelineOutput
from export.plot import plot_step_means
from export.writers import write_csv, write_lines


def save_all_artifacts(results: PipelineOutput, out_dir: Path) -> Path | None:
    """
    Orchestrates the persistence of all analysis artifacts.
    Returns the path to the main plot if it was generated.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Writing outputs to: {out_dir}")

    # Save Data Tables
    _save_tables(results, out_dir)

    # Save Traceability IDs
    write_lines(results.comparison.base_request_ids, out_dir / "base_request_ids.txt")
    write_lines(results.comparison.opt_request_ids, out_dir / "opt_request_ids.txt")

    # Generate Visualizations
    return _generate_summary_plot(results, out_dir)


def _save_tables(results: PipelineOutput, out_dir: Path) -> None:
    """Internal helper to batch write all CSVs."""
    ex = results.extracts
    ev = results.events
    cmp = results.comparison

    # Raw / Transformed
    write_csv(ex.rest_requests, out_dir / "restpp_requests.csv")
    write_csv(ev.linked_events, out_dir / "gpe_events_attached.csv")
    write_csv(ev.step_timings, out_dir / "gaps_with_query.csv")

    # Analysis
    write_csv(cmp.request_summary, out_dir / "request_summary.csv")
    write_csv(cmp.execution_table, out_dir / "exec_request_table.csv")
    write_csv(cmp.step_statistics, out_dir / "step_stats.csv")
    write_csv(cmp.query_vs_query_stats, out_dir / "compare_two_queries.csv")
    write_csv(cmp.step_side_by_side, out_dir / "side_ordered_steps.csv")

    # Bottlenecks
    write_csv(cmp.bottlenecks_base, out_dir / "bottlenecks_base.csv")
    write_csv(cmp.bottlenecks_opt, out_dir / "bottlenecks_opt.csv")


def _generate_summary_plot(results: PipelineOutput, out_dir: Path) -> Path | None:
    """Internal helper to determine if we can/should plot."""
    side = results.comparison.step_side_by_side

    if (
        side.empty
        or "base_mean_ms" not in side.columns
        or "opt_mean_ms" not in side.columns
    ):
        return None

    plot_path = out_dir / "step_means_base_vs_opt.png"
    plot_step_means(
        side,
        out_path=plot_path,
        title="Per-step mean duration: Base vs Optimized",
    )
    return plot_path
