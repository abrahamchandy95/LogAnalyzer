from pathlib import Path


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
