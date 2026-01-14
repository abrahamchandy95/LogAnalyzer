import pandas as pd
from src.interfaces import ReportPresenter
from src.domain import BenchmarkReport


class SummaryPresenter(ReportPresenter):
    """
    Prints text summaries to stdout.
    """

    def present(self, report: BenchmarkReport) -> None:
        print("\n" + "=" * 40)
        print(f"BENCHMARK REPORT: {report.base_variant} vs {report.candidate_variant}")
        print("=" * 40)

        self._print_bottlenecks("Base", report.base_bottlenecks)
        self._print_bottlenecks("Candidate", report.candidate_bottlenecks)

    def _print_bottlenecks(self, variant_name: str, df: pd.DataFrame) -> None:
        print(f"\n--- Top Bottlenecks ({variant_name}) ---")
        if df.empty:
            print("No data found.")
            return

        # Select readable columns
        cols = ["gap_ms", "step_key", "detail"]
        # Ensure columns exist before printing to avoid crashes
        existing_cols = [c for c in cols if c in df.columns]

        print(df[existing_cols].head(5).to_string(index=False))
