"""
Implements the LatencyComparator protocol
"""

from typing import cast

import pandas as pd
from src.interfaces import LatencyComparator
from src.domain import BenchmarkReport
from src.analysis.stats import enrich_restpp_data, calculate_step_gaps
from src.interfaces import QueryName
from src.utils import pct


class QueryLatencyComparator(LatencyComparator):
    """
    Aggregates step-level latency metrics and compares two query variants
    """

    def compare_variants(
        self,
        restpp_data: pd.DataFrame,
        gpe_data: pd.DataFrame,
        base: QueryName,
        candidate: QueryName,
    ) -> BenchmarkReport:
        req_summary = enrich_restpp_data(restpp_data)
        gaps = calculate_step_gaps(gpe_data, req_summary)

        gaps = cast(
            pd.DataFrame, gaps[gaps["query_name"].isin([base, candidate])].copy()
        )
        if gaps.empty:
            return self._build_empty_report(base, candidate)
        stats = self._aggregate_step_stats(gaps)
        comparison_df = self._pivot_and_calculate_deltas(stats, base, candidate)
        bottlenecks = (
            gaps.sort_values("gap_ms", ascending=False).groupby("query_name").head(10)
        )
        return BenchmarkReport(
            base_variant=base,
            candidate_variant=candidate,
            step_latency_comparison=comparison_df,
            base_bottlenecks=cast(
                pd.DataFrame, bottlenecks[bottlenecks["query_name"] == base]
            ),
            candidate_bottlenecks=cast(
                pd.DataFrame, bottlenecks[bottlenecks["query_name"] == candidate]
            ),
        )

    def _pivot_and_calculate_deltas(
        self, stats: pd.DataFrame, base: str, candidate: str
    ) -> pd.DataFrame:
        """
        Pivots the stats table to side-by-side columns and computes diffs.
        """
        # Pivot: rows=Step, cols=QueryName, values=Metrics
        pivot = stats.pivot(
            index=["step_key", "iteration"],
            columns="query_name",
            values=["mean_ms", "median_ms", "n"],
        )

        # Flatten MultiIndex columns: e.g. ('mean_ms', 'base_query') -> 'base_query_mean_ms'
        # Note: We reverse the order (c[1] first) so it reads "QueryName_Metric"
        pivot.columns = [f"{c[1]}_{c[0]}" for c in pivot.columns]
        pivot.reset_index(inplace=True)

        # Calculate Delta Columns dynamically
        base_col = f"{base}_mean_ms"
        cand_col = f"{candidate}_mean_ms"

        if base_col in pivot.columns and cand_col in pivot.columns:
            pivot["diff_mean_ms"] = pivot[cand_col] - pivot[base_col]

            # Avoid division by zero
            pivot["ratio_mean"] = pivot.apply(
                lambda row: row[cand_col] / row[base_col] if row[base_col] > 0 else 0.0,
                axis=1,
            )

            # Sort by biggest regression (positive diff) first
            return cast(
                pd.DataFrame, pivot.sort_values("diff_mean_ms", ascending=False)
            )

        return cast(pd.DataFrame, pivot)

    def _aggregate_step_stats(self, gaps: pd.DataFrame) -> pd.DataFrame:
        """
        Computes stats per step
        """
        return cast(
            pd.DataFrame,
            gaps.groupby(["query_name", "step_key", "iteration"], dropna=False)
            .agg(
                n=("gap_ms", "count"),
                mean_ms=("gap_ms", "mean"),
                median_ms=("gap_ms", lambda s: pct(s, 50)),
                p95_ms=("gap_ms", lambda s: pct(s, 95)),
            )
            .reset_index(),
        )

    def _build_empty_report(
        self, base: QueryName, candidate: QueryName
    ) -> BenchmarkReport:
        return BenchmarkReport(
            base_variant=base,
            candidate_variant=candidate,
            step_latency_comparison=pd.DataFrame(),
            base_bottlenecks=pd.DataFrame(),
            candidate_bottlenecks=pd.DataFrame(),
        )
