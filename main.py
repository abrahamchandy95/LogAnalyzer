import sys
from pathlib import Path

from cli import parse_cli_args
from common.support.env import load_env_config
from common.support.reporting import PrintReporter, Reporter
from export.artifacts import save_all_artifacts
from export.open_file import open_file
from pipeline import run_performance_analysis


def main() -> int:
    reporter: Reporter = PrintReporter()

    if len(sys.argv) > 1:
        reporter.info("CLI arguments detected. Using cli.py parser...")
        app_config = parse_cli_args()
    else:
        reporter.info("No arguments detected. Using .env configuration...")
        repo_root = Path(__file__).resolve().parent
        env_file = repo_root / ".env"
        app_config = load_env_config(env_path=env_file)

    cfg = app_config.cfg
    open_plot = app_config.open_plot

    reporter.info("--- Starting Performance Analysis ---")
    results = run_performance_analysis(cfg, reporter=reporter)

    reporter.info("--- Saving Artifacts ---")
    plot_path = save_all_artifacts(results, cfg.out_dir, reporter=reporter)

    reporter.info(f"Done. Output directory: {cfg.out_dir}")

    if open_plot and plot_path:
        reporter.info(f"Opening plot: {plot_path}")
        open_file(plot_path, reporter=reporter)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
