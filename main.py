import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Import from our modules
from src.domain import LogSource
from src.types import ExecutionID
from src.parsing.strategies import TigerGraphHeaderStrategy
from src.parsing.crawler import DirectoryLogCrawler
from src.parsing.restpp import parse_restpp_event
from src.parsing.gpe import parse_gpe_event
from src.analysis.comparator import QueryLatencyComparator
from src.analysis.repair import assign_request_ids_by_window
from src.analysis.metrics import calculate_execution_summary
from src.visualization.console import SummaryPresenter
from src.visualization.plotter import BarChartPresenter

# Configuration constants
NODES = ["m1", "m2", "m3", "m4"]


def parse_args():
    parser = argparse.ArgumentParser(description="TigerGraph Log Comparator")

    # Input Directories
    parser.add_argument(
        "--base-dir", type=Path, required=True, help="Path to logs for the BASE run"
    )
    parser.add_argument(
        "--cand-dir",
        type=Path,
        required=True,
        help="Path to logs for the CANDIDATE (Optimized) run",
    )

    # Query Names
    parser.add_argument(
        "--base-name", type=str, required=True, help="Query name for BASE variant"
    )
    parser.add_argument(
        "--cand-name", type=str, required=True, help="Query name for CANDIDATE variant"
    )

    # Config
    parser.add_argument("--plot", action="store_true", help="Show matplotlib chart")
    parser.add_argument(
        "--top-n", type=int, default=20, help="Number of bottlenecks to show"
    )

    return parser.parse_args()


def load_logs(run_dir: Path, run_id: ExecutionID) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orchestrates the crawling of one run directory.
    Returns (restpp_df, gpe_df).
    """
    print(f"Loading logs from {run_dir} (RunID: {run_id})...")

    header_strat = TigerGraphHeaderStrategy()

    # Instantiate Crawlers
    rest_crawler = DirectoryLogCrawler(
        glob_pattern="restpp*",
        line_processor=parse_restpp_event,
        header_strategy=header_strat,
    )

    gpe_crawler = DirectoryLogCrawler(
        glob_pattern="gpe*",
        line_processor=parse_gpe_event,
        header_strategy=header_strat,
    )

    all_rest = []
    all_gpe = []

    for node in NODES:
        source = LogSource(execution_id=run_id, server_node=node, file_path=run_dir)

        # Parse
        print(f"  Scanning {node}...")
        all_rest.append(rest_crawler.parse(source))
        all_gpe.append(gpe_crawler.parse(source))

    # Combine
    rest_df = (
        pd.concat(all_rest, ignore_index=True)
        if any(not x.empty for x in all_rest)
        else pd.DataFrame()
    )
    gpe_df = (
        pd.concat(all_gpe, ignore_index=True)
        if any(not x.empty for x in all_gpe)
        else pd.DataFrame()
    )

    return rest_df, gpe_df


def print_execution_summary(
    metrics_df: pd.DataFrame, base_id: str, cand_id: str, base_name: str, cand_name: str
):
    """
    Prints high-level stats (Avg Latency, Queue Delay) to console.
    """
    print("\n" + "=" * 50)
    print("HIGH-LEVEL EXECUTION SUMMARY")
    print("=" * 50)

    # Filter metrics by run
    base_m = metrics_df[metrics_df["run"] == base_id]
    cand_m = metrics_df[metrics_df["run"] == cand_id]

    # Helper to calculate mean safe
    def get_mean(df, col):
        return df[col].mean() if col in df.columns else np.nan

    print(
        f"{'Metric':<25} | {'Base (' + base_name + ')':<30} | {'Candidate (' + cand_name + ')':<30} | {'Diff':<10}"
    )
    print("-" * 100)

    metrics = [
        ("Avg Actual GPE Time", "actual_gpe_ms"),
        ("Avg Reported Time", "reported_udf_ms"),
        ("Avg Post-Proc Overhead", "post_process_overhead_ms"),
    ]

    for label, col in metrics:
        b_val = get_mean(base_m, col)
        c_val = get_mean(cand_m, col)
        diff = c_val - b_val
        print(f"{label:<25} | {b_val:>10.2f} ms | {c_val:>10.2f} ms | {diff:>+8.2f} ms")

    print("-" * 100)
    print(f"Request Count | {len(base_m):>10} | {len(cand_m):>10} |")
    print("\n")


def main():
    args = parse_args()

    #  Validation
    if not args.base_dir.exists():
        sys.exit(f"Error: Base directory not found: {args.base_dir}")
    if not args.cand_dir.exists():
        sys.exit(f"Error: Candidate directory not found: {args.cand_dir}")

    #  Load Data
    BASE_RUN_ID: ExecutionID = "base_run"
    CAND_RUN_ID: ExecutionID = "cand_run"

    base_rest, base_gpe = load_logs(args.base_dir, BASE_RUN_ID)
    cand_rest, cand_gpe = load_logs(args.cand_dir, CAND_RUN_ID)

    if base_gpe.empty or cand_gpe.empty:
        sys.exit("Error: No GPE logs found in one of the directories.")

    #  Pre-processing / Merging
    print("Merging and repairing log streams...")
    full_rest = pd.concat([base_rest, cand_rest], ignore_index=True)
    full_gpe = pd.concat([base_gpe, cand_gpe], ignore_index=True)

    # Apply Session Repair (Notebook Logic: Window-based ID filling)
    full_gpe = assign_request_ids_by_window(full_gpe)

    metrics_df = calculate_execution_summary(full_gpe, full_rest)
    print_execution_summary(
        metrics_df, BASE_RUN_ID, CAND_RUN_ID, args.base_name, args.cand_name
    )

    # 4. Step-by-Step Analysis
    print(f"Comparing Steps: {args.base_name} vs {args.cand_name}...")
    comparator = QueryLatencyComparator()

    report = comparator.compare_variants(
        restpp_data=full_rest,
        gpe_data=full_gpe,
        base=args.base_name,
        candidate=args.cand_name,
    )

    #  Visualization
    # Console Summary (Bottlenecks)
    console_presenter = SummaryPresenter()
    console_presenter.present(report)

    output_dir = Path("../_analysis_outputs")
    output_dir.mkdir(exist_ok=True)

    # Matplotlib Chart
    if args.plot:
        plotter = BarChartPresenter(top_n=args.top_n)
        plot_path = output_dir / "latency_comparison.png"
        plotter.present(report, save_path=plot_path)

    out_csv = output_dir / "comparison_results.csv"

    report.step_latency_comparison.to_csv(out_csv, index=False)
    print(f"\nDetailed step comparison saved to {out_csv}")


if __name__ == "__main__":
    main()
