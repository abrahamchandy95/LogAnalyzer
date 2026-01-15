from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_step_means(
    step_data: pd.DataFrame, *, out_path: Path | None = None, title: str = ""
) -> None:
    """
    Generic plotting function for side-by-side bar charts.
    """
    # Create a copy to avoid mutating the input dataframe
    plot_df = step_data.copy()

    # Determine sorting column
    sort_col = "pos" if "pos" in plot_df.columns else "step_key"
    plot_df = plot_df.sort_values(by=sort_col, na_position="last")

    # Data prep
    x = np.arange(len(plot_df))
    w = 0.45
    base = plot_df["base_mean_ms"].to_numpy()
    opt = plot_df["opt_mean_ms"].to_numpy()
    labels = plot_df["step_key"].astype(str).to_list()

    # Plotting
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - w / 2, base, w, label="base")
    ax.bar(x + w / 2, opt, w, label="optimized")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=90, fontsize=8)
    ax.set_ylabel("Mean time between LOG steps (ms)")
    ax.set_title(title or "Per-step mean duration: Base vs Optimized")
    ax.legend()
    fig.tight_layout()

    # Output handling
    try:
        if out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(out_path)
        else:
            plt.show()
    finally:
        plt.close(fig)
