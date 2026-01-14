from typing import Protocol
from dataclasses import dataclass
from pathlib import Path

import re
import pandas as pd


@dataclass(frozen=True)
class LogHeader:
    """
    Represents a log line's prefix
    """

    timestamp: pd.Timestamp
    thread_id: int
    payload: str


class HeaderStrategy(Protocol):
    """
    Defines how to extract metadata from a line
    """

    def parse(self, line: str, file: Path, year: int) -> LogHeader | None: ...


class TigerGraphHeaderStrategy:
    """
    Parses standard TigerGraph log headers.
    Format: IMMDD hh:mm:ss.ms thread_id ...
    """

    _RX_HEADER = re.compile(
        r"^I(?P<mm>\d{2})(?P<dd>\d{2})\s+(?P<hms>\d{2}:\d{2}:\d{2}\.\d+)\s+(?P<tid>\d+)\s+.*?\]\s+(?P<msg>.*)$"
    )

    def parse(self, line: str, file: Path, year: int) -> LogHeader | None:
        if not line.startswith("I"):
            return None

        m = self._RX_HEADER.match(line)
        if not m:
            return None

        ts_str = f"{year}-{m.group('mm')}-{m.group('dd')} {m.group('hms')}"

        try:
            ts = pd.Timestamp(ts_str)
        except ValueError:
            return None

        if ts is pd.NaT:
            return None
        if not isinstance(ts, pd.Timestamp):
            return None

        return LogHeader(
            timestamp=ts,
            thread_id=int(m.group("tid")),
            payload=m.group("msg"),
        )
