from pathlib import Path

import pandas as pd


def top_bottlenecks(
    gaps_with_qname: pd.DataFrame, query_name: str, *, n: int = 50
) -> pd.DataFrame:
    """
    Top slowest individual gaps for a given query variant.
    Includes (log_path, lineno) to jump to the exact log line.
    """
    g = gaps_with_qname.loc[gaps_with_qname["query_name"] == query_name].copy()
    if g.empty:
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "query_name",
                    "gap_ms",
                    "step_key",
                    "iteration",
                    "run",
                    "node",
                    "tid",
                    "request_id",
                    "ts",
                    "prev_label",
                    "event",
                    "label",
                    "log_path",
                    "lineno",
                    "detail",
                ]
            )
        )

    cols = [
        "query_name",
        "gap_ms",
        "step_key",
        "iteration",
        "run",
        "node",
        "tid",
        "request_id",
        "ts",
        "prev_label",
        "event",
        "label",
        "log_path",
        "lineno",
        "detail",
    ]
    keep = [c for c in cols if c in g.columns]
    return g.sort_values("gap_ms", ascending=False).head(n)[keep]


def show_log_context(log_path: str | Path, lineno: int, *, context: int = 3) -> None:
    """
    Print a few lines around a suspicious log line.
    """
    p = Path(log_path)
    if not p.exists():
        print(f"Missing file: {p}")
        return

    start = max(1, lineno - context)
    end = lineno + context

    with p.open("r", errors="replace") as f:
        for i, line in enumerate(f, start=1):
            if i < start:
                continue
            if i > end:
                break
            prefix = ">>" if i == lineno else "  "
            print(f"{prefix} {i:6d}: {line.rstrip()}")
