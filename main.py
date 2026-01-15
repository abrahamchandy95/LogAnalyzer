from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# --- Internal Imports ---
from analysis.bottlenecks import top_bottlenecks
from analysis.requests import build_exec_request_table, extract_ids, summarize_requests
from analysis.stats import (
    build_ordered_step_side_table,
    compare_two_queries,
    make_step_stats,
)
from export.plot import open_file_in_default_app, plot_step_means
from export.writers import write_csv, write_lines
from parsers.gpe import parse_gpe
from parsers.restpp import parse_restpp
from transforms.attach import attach_steps_to_requests
from transforms.gaps import add_query_name, build_gaps

# --- Config & Type Imports ---
from cli import parse_args
from common.env import load_env_config
from common.types import RunInput


def run_pipeline(
    *,
    out_dir: Path,
    runs: tuple[RunInput, ...],
    nodes: tuple[str, ...],
    base_query: str,
    opt_query: str,
) -> Path | None:
    """
    The core analysis logic.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Parse logs
    all_rest: list[pd.DataFrame] = []
    all_gpe: list[pd.DataFrame] = []

    for run in runs:
        print(f"Parsing run: {run.key} -> {run.path}")
        all_rest.append(parse_restpp(run.key, run.path, nodes=nodes))
        all_gpe.append(parse_gpe(run.key, run.path, nodes=nodes))

    restpp_req = pd.concat(all_rest, ignore_index=True) if all_rest else pd.DataFrame()
    gpe_events = pd.concat(all_gpe, ignore_index=True) if all_gpe else pd.DataFrame()

    # 2. Transforms
    print("Running transforms...")
    gpe_attached = attach_steps_to_requests(gpe_events)
    gaps = build_gaps(gpe_attached)
    gapsq = add_query_name(gaps, restpp_req)

    # 3. Analysis
    print("Running analysis...")
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

    # 4. Write outputs
    print(f"Writing outputs to: {out_dir}")
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

    # 5. Plot
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


def main() -> int:
    # --- CONFIGURATION SWITCHING LOGIC ---

    # Check if arguments were passed (len > 1 because argv[0] is the script name)
    if len(sys.argv) > 1:
        print("CLI arguments detected. Using cli.py parser...")
        # parse_args returns (CompareConfig, bool)
        cfg, open_plot = parse_args()

    else:
        print("No arguments detected. Using .env configuration...")
        repo_root = Path(__file__).resolve().parent
        env_file = repo_root / ".env"

        # load_env_config returns EnvConfig object
        envcfg = load_env_config(env_path=env_file)

        # Unpack EnvConfig
        cfg = envcfg.cfg
        open_plot = envcfg.open_plot

    # --- EXECUTION ---

    plot_path = run_pipeline(
        out_dir=cfg.out_dir,
        runs=cfg.runs,
        nodes=cfg.nodes,
        base_query=cfg.base_query,
        opt_query=cfg.opt_query,
    )

    print("Done.")

    if open_plot and plot_path is not None:
        print(f"Opening plot: {plot_path}")
        open_file_in_default_app(plot_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
