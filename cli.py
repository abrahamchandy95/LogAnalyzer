import argparse
from pathlib import Path

from common.model.config import AppConfig, CompareConfig
from common.model.types import RunInput


def _parse_run_arg(arg_value: str) -> RunInput:
    """
    Parses a run argument string in the format 'KEY=PATH'.
    Example: --run 2025-12-11=/abs/path/to/logdir
    """
    if "=" not in arg_value:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{arg_value}'. Run must be KEY=PATH (e.g., base=/path/to/logs)"
        )

    id, path_str = arg_value.split("=", 1)

    # Resolving path immediately ensures we fail fast if the path is invalid
    # though strict existence checking is usually done in the pipeline
    path = Path(path_str).expanduser().resolve()

    return RunInput(id=id.strip(), path=path)


def _get_default_output_dir() -> Path:
    """
    Calculates a sensible default output directory relative to the repository.
    """
    # .../QueryOptimizations/LogAnalyzer/cli.py
    repo_root = Path(__file__).resolve().parent

    # .../QueryOptimizations/LogAnalyzer_outputs
    return repo_root.parent / "LogAnalyzer_outputs"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="loganalyzer",
        description="Compare two query variants from RESTPP/GPE logs.",
    )

    parser.add_argument(
        "--run",
        action="append",
        type=_parse_run_arg,
        required=True,
        help="Run definition in KEY=PATH format (e.g. 'base=/tmp/logs'). Can be repeated.",
        dest="runs",  # explicit destination name
    )

    parser.add_argument(
        "--nodes",
        nargs="+",
        default=["m1", "m2", "m3", "m4"],
        help="List of node names to parse (default: m1 m2 m3 m4)",
    )

    parser.add_argument(
        "--base-query",
        required=True,
        help="Name of the baseline query (for stats filtering)",
    )

    parser.add_argument(
        "--opt-query",
        required=True,
        help="Name of the optimized query (for stats filtering)",
    )

    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directory to save artifacts. Defaults to ../LogAnalyzer_outputs",
    )

    parser.add_argument(
        "--open-plot",
        action="store_true",
        help="Automatically open the generated plot when finished",
    )

    return parser


def parse_cli_args(argv: list[str] | None = None) -> AppConfig:
    """
    Parses command line arguments and returns a unified AppConfig object.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Convert list back to tuple for immutability
    runs = tuple(args.runs)
    nodes = tuple(str(n) for n in args.nodes)

    # Determine output directory
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else _get_default_output_dir()
    )

    cfg = CompareConfig(
        runs=runs,
        nodes=nodes,
        base_query=str(args.base_query),
        opt_query=str(args.opt_query),
        out_dir=out_dir,
    )

    return AppConfig(cfg=cfg, open_plot=bool(args.open_plot))
