from pathlib import Path

import pandas as pd

from analysis.bottlenecks import top_bottlenecks
from analysis.requests import build_exec_request_table, extract_ids, summarize_requests
from analysis.stats import (
    build_ordered_step_side_table,
    compare_two_queries,
    make_step_stats,
)
from cli import parse_args
from export.plot import open_file_in_default_app, plot_step_means
from export.writers import write_csv, write_lines
from parsers.gpe import parse_gpe
from parsers.restpp import parse_restpp
from transforms.attach import attach_steps_to_requests
from transforms.gaps import add_query_name, build_gaps


def run_pipeline(
    *, out_dir: Path, runs, nodes, base_query: str, opt_query: str
) -> Path | None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Parse logs
    all_rest: list[pd.DataFrame] = []
    all_gpe: list[pd.DataFrame] = []

    for run in runs:
        all_rest.append(parse_restpp(run.key, run.path, nodes=nodes))
        all_gpe.append(parse_gpe(run.key, run.path, nodes=nodes))

    restpp_req = pd.concat(all_rest, ignore_index=True) if all_rest else pd.DataFrame()
    gpe_events = pd.concat(all_gpe, ignore_index=True) if all_gpe else pd.DataFrame()

    # Transforms
    gpe_attached = attach_steps_to_requests(gpe_events)
    gaps = build_gaps(gpe_attached)
    gapsq = add_query_name(gaps, restpp_req)

    # Analysis
    req_summary = summarize_requests(restpp_req, gpe_attached)
    exec_tbl = build_exec_request_table(restpp_req, gpe_attached)
    base_ids, opt_ids = extract_ids(exec_tbl, base_query, opt_query)

    step_stats = make_step_stats(gapsq)
    compare = compare_two_queries(step_stats, base_query, opt_query)
    side = build_ordered_step_side_table(
        gapsq, base_query=base_query, opt_query=opt_query
    )

    bott_base = top_bottlenecks(gapsq, base_query, n=50)
    bott_opt = top_bottlenecks(gapsq, opt_query, n=50)

    # Write outputs (outside repo)
    write_csv(restpp_req, out_dir / "restpp_requests.csv")
    write_csv(gpe_attached, out_dir / "gpe_events_attached.csv")
    write_csv(gapsq, out_dir / "gaps_with_query.csv")
    write_csv(req_summary, out_dir / "request_summary.csv")
    write_csv(exec_tbl, out_dir / "exec_request_table.csv")

    write_csv(step_stats, out_dir / "step_stats.csv")
    write_csv(compare, out_dir / "compare_two_queries.csv")
    write_csv(side, out_dir / "side_ordered_steps.csv")

    write_csv(bott_base, out_dir / "bottlenecks_base.csv")
    write_csv(bott_opt, out_dir / "bottlenecks_opt.csv")

    write_lines(base_ids, out_dir / "base_request_ids.txt")
    write_lines(opt_ids, out_dir / "opt_request_ids.txt")

    # Plot (optional if side empty)
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

    return None


def main(argv: list[str] | None = None) -> int:
    cfg, open_plot = parse_args(argv)

    plot_path = run_pipeline(
        out_dir=cfg.out_dir,
        runs=cfg.runs,
        nodes=cfg.nodes,
        base_query=cfg.base_query,
        opt_query=cfg.opt_query,
    )

    print(f"Outputs written to: {cfg.out_dir}")

    if open_plot and plot_path is not None:
        open_file_in_default_app(plot_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
