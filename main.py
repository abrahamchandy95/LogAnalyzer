from __future__ import annotations

import sys
from pathlib import Path

from cli import parse_args
from common.env import load_env_config
from export.plot import open_file_in_default_app

from pipeline import execute_pipeline, save_artifacts


def main() -> int:
    # Check if arguments were passed
    if len(sys.argv) > 1:
        print("CLI arguments detected. Using cli.py parser...")
        cfg, open_plot = parse_args()
    else:
        print("No arguments detected. Using .env configuration...")
        repo_root = Path(__file__).resolve().parent
        env_file = repo_root / ".env"

        envcfg = load_env_config(env_path=env_file)
        cfg = envcfg.cfg
        open_plot = envcfg.open_plot

    print("\n--- Starting Analysis Pipeline ---")

    results = execute_pipeline(cfg)

    print("\n--- Saving Artifacts ---")

    plot_path = save_artifacts(results, cfg.out_dir)

    print(f"\nDone. Output directory: {cfg.out_dir}")

    if open_plot and plot_path:
        print(f"Opening plot: {plot_path}")
        open_file_in_default_app(plot_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
