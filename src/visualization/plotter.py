from typing import cast
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from src.domain import BenchmarkReport
from src.interfaces import QueryName, ReportPresenter


class BarChartPresenter(ReportPresenter):
    def __init__(self, top_n: int = 20, fig_size: tuple[int, int] = (12, 6)):
        self.top_n = top_n
        self.fig_size = fig_size

    def present(self, report: BenchmarkReport, save_path: Path | None = None) -> None:
        plot_df = self._prepare_data(report)
        if plot_df.empty:
            print(
                f"Skipping plot: Insufficient intersection between "
                f"{report.base_variant} and {report.candidate_variant}"
            )
            return

        self._render_plot(
            plot_df, report.base_variant, report.candidate_variant, save_path
        )

    def _prepare_data(self, report: BenchmarkReport) -> pd.DataFrame:
        """
        Selects top N slowest steps common to both queries
        """
        df = report.step_latency_comparison.copy()
        base_column = f"{report.base_variant}_mean_ms"
        cand_column = f"{report.candidate_variant}_mean_ms"

        if base_column not in df.columns or cand_column not in df.columns:
            return pd.DataFrame()

        df = df.dropna(subset=[base_column, cand_column])
        df = df.sort_values(base_column, ascending=False).head(self.top_n).copy()

        df["step_label"] = (
            df["step_key"].astype(str)
            + " (it:"
            + df["iteration"].fillna(0).astype(int).astype(str)
            + ")"
        )
        return df

    def _render_plot(
        self, df: pd.DataFrame, base: QueryName, cand: QueryName, save_path: Path | None
    ) -> None:
        base_col = f"{base}_mean_ms"
        cand_col = f"{cand}_mean_ms"

        plt.figure(figsize=self.fig_size)
        x = range(len(df))
        width = 0.35

        # Plot Bars
        plt.bar([i - width / 2 for i in x], df[base_col], width, label=f"Base: {base}")
        plt.bar(
            [i + width / 2 for i in x],
            df[cand_col],
            width,
            label=f"Candidate: {cand}",
        )

        # Decoration
        plt.xlabel("Step")
        plt.ylabel("Mean Latency (ms)")
        plt.title(f"Step Latency Comparison (Top {len(df)} Slowest Steps)")
        labels = cast(list[str], df["step_label"].tolist())
        plt.xticks(list(x), labels, rotation=45, ha="right")
        plt.legend()
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path)
            print(f"\nPlot saved to: {save_path}")
            plt.close()
        else:
            plt.show()
