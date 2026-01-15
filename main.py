import sys
from pathlib import Path

from cli import parse_cli_args
from common.reporting import PrintReporter
from common.env import load_env_config
from export.artifacts import save_all_artifacts
from export.plot import open_file_in_default_app
from pipeline import run_performance_analysis


def main() -> int:
    if len(sys.argv) > 1:
        print("CLI arguments detected. Using cli.py parser...")
        app_config = parse_cli_args()
    else:
        print("No arguments detected. Using .env configuration...")
        repo_root = Path(__file__).resolve().parent
        env_file = repo_root / ".env"
        app_config = load_env_config(env_path=env_file)

    # Now unpack attributes from the object
    cfg = app_config.cfg
    open_plot = app_config.open_plot

    print("\n--- Starting Performance Analysis ---")

    results = run_performance_analysis(cfg, reporter=PrintReporter())

    print("\n--- Saving Artifacts ---")
    plot_path = save_all_artifacts(results, cfg.out_dir)
    print(f"\nDone. Output directory: {cfg.out_dir}")

    if open_plot and plot_path:
        print(f"Opening plot: {plot_path}")
        open_file_in_default_app(plot_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
