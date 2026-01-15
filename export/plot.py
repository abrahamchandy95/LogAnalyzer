from pathlib import Path

import sys
import subprocess
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_step_means(
    side: pd.DataFrame, *, out_path: Path | None = None, title: str = ""
) -> None:
    plot_df = side.copy()

    sort_col: str = "pos" if "pos" in plot_df.columns else "step_key"
    plot_df = plot_df.sort_values(by=sort_col, na_position="last")

    x = np.arange(len(plot_df))
    w = 0.45

    base = plot_df["base_mean_ms"].to_numpy()
    opt = plot_df["opt_mean_ms"].to_numpy()
    labels = plot_df["step_key"].astype(str).to_list()

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - w / 2, base, w, label="base")
    ax.bar(x + w / 2, opt, w, label="optimized")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=90, fontsize=8)
    ax.set_ylabel("Mean time between LOG steps (ms)")
    ax.set_title(title or "Per-step mean duration: Base vs Optimized")
    ax.legend()
    fig.tight_layout()

    try:
        if out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(out_path)
        else:
            plt.show()
    finally:
        plt.close(fig)


def open_file_in_default_app(path: Path) -> None:
    """
    Best-effort: open a file with the OS default viewer (non-blocking).
    """
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        elif os.name == "nt":
            # os.startfile is Windows-only; keep typing strict
            getattr(os, "startfile")(str(path))  # type: ignore[misc]
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as e:
        print(f"WARNING: failed to open file: {path} ({e})")
