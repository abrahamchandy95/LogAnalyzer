from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class OutputPaths:
    out_dir: Path

    # tables
    restpp_requests_csv: Path
    gpe_events_attached_csv: Path
    gaps_with_query_csv: Path

    request_summary_csv: Path
    exec_request_table_csv: Path
    step_stats_csv: Path
    compare_two_queries_csv: Path
    side_ordered_steps_csv: Path

    bottlenecks_base_csv: Path
    bottlenecks_opt_csv: Path

    # traceability
    base_request_ids_txt: Path
    opt_request_ids_txt: Path

    # plot
    step_means_png: Path


def build_output_paths(out_dir: Path) -> OutputPaths:
    od = out_dir.resolve()
    return OutputPaths(
        out_dir=od,
        restpp_requests_csv=od / "restpp_requests.csv",
        gpe_events_attached_csv=od / "gpe_events_attached.csv",
        gaps_with_query_csv=od / "gaps_with_query.csv",
        request_summary_csv=od / "request_summary.csv",
        exec_request_table_csv=od / "exec_request_table.csv",
        step_stats_csv=od / "step_stats.csv",
        compare_two_queries_csv=od / "compare_two_queries.csv",
        side_ordered_steps_csv=od / "side_ordered_steps.csv",
        bottlenecks_base_csv=od / "bottlenecks_base.csv",
        bottlenecks_opt_csv=od / "bottlenecks_opt.csv",
        base_request_ids_txt=od / "base_request_ids.txt",
        opt_request_ids_txt=od / "opt_request_ids.txt",
        step_means_png=od / "step_means_base_vs_opt.png",
    )
