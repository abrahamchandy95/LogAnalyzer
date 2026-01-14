from collections.abc import Callable
from pathlib import Path
import pandas as pd

from src.domain import LogSource, LogEvent
from src.interfaces import LogParser
from src.parsing.strategies import HeaderStrategy, LogHeader
from src.utils import detect_year_from_file_header

# A function that takes a parsed header + line number, and returns a data dict
type LineProcessor = Callable[[LogHeader, int], LogEvent | None]


class DirectoryLogCrawler(LogParser):
    """
    A generic log crawler that:
     Finds files matching a glob pattern (e.g., 'gpe*').
     Uses a HeaderStrategy to read the timestamp/thread info.
     Uses a LineProcessor to extract specific event data.
    """

    def __init__(
        self,
        glob_pattern: str,
        line_processor: LineProcessor,
        header_strategy: HeaderStrategy,
    ):
        self.glob_pattern = glob_pattern
        self.line_processor = line_processor
        self.strategy = header_strategy

    def parse(self, source: LogSource) -> pd.DataFrame:
        """
        Scans the directory defined in LogSource and parses all matching files.
        """
        # Construct the full path to the node's logs
        target_dir = source.file_path / source.server_node

        if not target_dir.exists():
            return pd.DataFrame()

        rows: list[LogEvent] = []

        # Iterate over all files matching the pattern
        for log_file in target_dir.glob(self.glob_pattern):
            if log_file.is_file():
                file_data = self._scan_file(log_file, source)
                rows.extend(file_data)

        return pd.DataFrame(rows)

    def _scan_file(self, log_file: Path, source: LogSource) -> list[LogEvent]:
        """
        Opens a specific file, detects its year, and parses line-by-line.
        """
        results: list[LogEvent] = []

        year = detect_year_from_file_header(log_file, default_year=2026)

        with log_file.open("r", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                header = self.strategy.parse(line, log_file, year)
                if not header:
                    continue

                data = self.line_processor(header, lineno)

                if data:
                    data["run"] = source.execution_id
                    data["node"] = source.server_node
                    data["ts"] = header.timestamp
                    data["tid"] = header.thread_id
                    data["log_path"] = str(log_file)

                    results.append(data)
        return results
