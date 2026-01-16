from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import NamedTuple

type RunId = str
type Node = str
type RequestId = str
type QueryName = str


class GlogEntry(NamedTuple):
    ts: datetime
    tid: int
    msg: str


@dataclass(frozen=True, slots=True)
class RunInput:
    id: RunId
    path: Path
