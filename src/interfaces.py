from typing import Protocol, runtime_checkable
import pandas as pd
from src.domain import BenchmarkReport, LogSource
from src.types import QueryName


@runtime_checkable
class LogParser(Protocol):
    """
    Reads logs from a directory and converts into a pandas DataFrame
    """

    def parse(self, source: LogSource) -> pd.DataFrame: ...


@runtime_checkable
class LatencyComparator(Protocol):
    """
    Domain Service: Encapsulates Logic to calculate step latency and generate
    statistical comparisons between two query variants.
    """

    def compare_variants(
        self,
        restpp_data: pd.DataFrame,
        gpe_data: pd.DataFrame,
        base: QueryName,
        candidate: QueryName,
    ) -> BenchmarkReport: ...


@runtime_checkable
class ReportPresenter(Protocol):
    """
    Responsible for final output format. Decouples the math from the view
    """

    def present(self, report: BenchmarkReport) -> None: ...
