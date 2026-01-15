from pathlib import Path
from typing import Iterable

import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(df: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=index)


def write_lines(lines: Iterable[str], path: Path) -> None:
    ensure_dir(path.parent)
    text = "\n".join(lines)
    if text and not text.endswith("\n"):
        text += "\n"
    path.write_text(text, encoding="utf-8")
