from dataclasses import dataclass
from typing import cast
import pandas as pd


@dataclass
class SignatureMatch:
    target_request_id: str
    candidate_request_id: str
    score: float
    debug_diff: pd.DataFrame


class RequestFingerprinter:
    """
    Domain Service: Encapsulates the logic for extracting an execution
    signature from a request and calculating similarity scores.
    """

    def __init__(self, step_whitelist: set[str] | None = None):
        self.step_whitelist = step_whitelist

    def build_signature(
        self, gaps_df: pd.DataFrame, run: str, request_id: str
    ) -> pd.DataFrame:
        """Creates a signature based on step counts and iterations."""

        #  Build mask safely
        mask = (gaps_df["run"] == run) & (gaps_df["request_id"] == request_id)

        if self.step_whitelist:
            mask &= gaps_df["step_key"].isin(list(self.step_whitelist))

        g = gaps_df[mask].copy()

        if g.empty:
            return pd.DataFrame(
                data=None,
                columns=pd.Index(["step_key", "count", "iteration_count", "sum_ms"]),
            )
        sig = cast(
            pd.DataFrame,
            g.groupby("step_key", as_index=False).agg(
                count=("gap_ms", "size"),
                max_iter=("iteration", "max"),
                sum_ms=("gap_ms", "sum"),
            ),
        )

        # Calculate iteration count (max_iter + 1)
        sig["iteration_count"] = sig["max_iter"].fillna(-1).astype(int) + 1

        return sig

    def score(
        self, target_sig: pd.DataFrame, candidate_sig: pd.DataFrame
    ) -> tuple[float, pd.DataFrame]:
        """
        Calculates distance between two signatures. Lower is better.
        """
        # Rename for merge
        t = target_sig.rename(columns=lambda x: f"t_{x}" if x != "step_key" else x)
        c = candidate_sig.rename(columns=lambda x: f"c_{x}" if x != "step_key" else x)

        m = pd.merge(t, c, on="step_key", how="outer").fillna(0)

        # Diff calculation
        m["diff_iter"] = (m["t_iteration_count"] - m["c_iteration_count"]).abs()
        m["diff_count"] = (m["t_count"] - m["c_count"]).abs()
        m["diff_ms"] = (m["t_sum_ms"] - m["c_sum_ms"]).abs()

        # Weighted Score (Iter > Count > Time)
        # This logic is isolated here, respecting SRP (Single Responsibility)
        score = (
            5.0 * m["diff_iter"].sum()
            + 1.0 * m["diff_count"].sum()
            + 0.01 * m["diff_ms"].sum()
        )

        return float(score), m.sort_values("diff_ms", ascending=False)
