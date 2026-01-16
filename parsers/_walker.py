from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, TypeIs, Iterable
from datetime import datetime

import pandas as pd
from pandas._libs import NaTType

from common.parse.time import infer_year_from_any_line_epoch
from common.parse.glog import parse_glog_line
from common.model.types import RunId, Node
from common.model.types import GlogEntry

type Timestampish = pd.Timestamp | NaTType


def is_timestamp(x: Timestampish) -> TypeIs[pd.Timestamp]:
    """
    Type-narrow helper for pandas typing stubs
    """
    return not isinstance(x, NaTType)


@dataclass(frozen=True, slots=True)
class ParsedLine:
    run: RunId
    node: Node
    log_path: Path
    lineno: int
    ts: pd.Timestamp
    tid: int
    msg: str


LineHandler = Callable[[ParsedLine], None]


class YearResolver(Protocol):
    def __call__(self, log_path: Path, *, default_year: int) -> int: ...


class GlogLineParser(Protocol):
    def __call__(self, line: str, *, year: int) -> GlogEntry | None: ...


class LogWalker(Protocol):
    def __call__(
        self,
        *,
        run_id: RunId,
        run_dir: Path,
        nodes: tuple[Node, ...],
        file_glob: str,
        on_line: LineHandler,
    ) -> None: ...


def _iter_log_paths(
    *, run_dir: Path, nodes: tuple[Node, ...], file_glob: str
) -> Iterable[tuple[Node, Path]]:
    """
    Yield (node, log_path) pairs for files matching file_glob.
    """
    for node in nodes:
        node_dir = run_dir / node
        if not node_dir.exists():
            continue

        for log_path in node_dir.glob(file_glob):
            if log_path.is_file():
                yield (node, log_path)


def walk_logs(
    *,
    run_id: RunId,
    run_dir: Path,
    nodes: tuple[Node, ...],
    file_glob: str,
    on_line: LineHandler,
    year_resolver: YearResolver = infer_year_from_any_line_epoch,
    glog_parser: GlogLineParser = parse_glog_line,
) -> None:
    """
    Walk log files and emit ParsedLine items to a caller-supplied handler.

    - SRP: traversal + decoding only.
    - DIP/OCP: year_resolver and glog_parser are injected dependencies.
    """
    default_year = datetime.now().year

    for node, log_path in _iter_log_paths(
        run_dir=run_dir, nodes=nodes, file_glob=file_glob
    ):
        file_year = year_resolver(log_path, default_year=default_year)

        with log_path.open("r", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                if line.startswith(">>>>>>>"):
                    continue

                gl = glog_parser(line, year=file_year)
                if gl is None:
                    continue

                ts_cand = pd.Timestamp(gl.ts)
                if not is_timestamp(ts_cand):
                    continue

                on_line(
                    ParsedLine(
                        run=run_id,
                        node=node,
                        log_path=log_path,
                        lineno=lineno,
                        ts=ts_cand,
                        tid=gl.tid,
                        msg=gl.msg,
                    )
                )
