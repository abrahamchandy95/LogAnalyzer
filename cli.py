import argparse
from pathlib import Path

from common.config import CompareConfig
from common.types import RunInput


def _parse_run(s: str) -> RunInput:
    """
    --run 2025-12-11=/abs/path/to/logdir
    """
    if "=" not in s:
        raise argparse.ArgumentTypeError(
            "run must be KEY=PATH (e.g. 2025-12-11=/path/to/dir)"
        )
    key, p = s.split("=", 1)
    return RunInput(key=key.strip(), path=Path(p).expanduser().resolve())


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="loganalyzer",
        description="Compare two query variants from RESTPP/GPE logs.",
    )
    p.add_argument(
        "--run",
        action="append",
        type=_parse_run,
        required=True,
        help="KEY=PATH (repeatable)",
    )
    p.add_argument("--nodes", nargs="+", default=["m1", "m2", "m3", "m4"])
    p.add_argument("--base-query", required=True)
    p.add_argument("--opt-query", required=True)
    p.add_argument(
        "--out-dir",
        default=None,
        help="Defaults to QueryOptimizations/LogAnalyzer_outputs",
    )
    p.add_argument("--open-plot", action="store_true", help="Open plot image when done")
    return p


def parse_args(argv: list[str] | None = None) -> tuple[CompareConfig, bool]:
    args = build_parser().parse_args(argv)

    runs = tuple(args.run)
    nodes = tuple(str(n) for n in args.nodes)

    repo_root = Path(__file__).resolve().parent  # .../QueryOptimizations/LogAnalyzer
    queryopt_root = repo_root.parent  # .../QueryOptimizations
    default_out = (
        queryopt_root / "LogAnalyzer_outputs"
    )  # .../QueryOptimizations/LogAnalyzer_outputs

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else default_out

    return (
        CompareConfig(
            runs=runs,
            nodes=nodes,
            base_query=str(args.base_query),
            opt_query=str(args.opt_query),
            out_dir=out_dir,
        ),
        bool(args.open_plot),
    )
