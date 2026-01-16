from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeIs

import pandas as pd
from pandas._libs import NaTType

from common.utils import detect_year_from_header, parse_glog_line
from common.types import RunId, Node

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


def walk_logs(
    *,
    run_id: RunId,
    run_dir: Path,
    nodes: tuple[Node, ...],
    file_glob: str,
    on_line: LineHandler,
) -> None:
    folder_year = int(run_id.split("-", 1)[0]) if "-" in run_id else 2026

    for node in nodes:
        node_dir = run_dir / node
        if not node_dir.exists():
            continue

        for log_path in node_dir.glob(file_glob):
            if not log_path.is_file():
                continue

            file_year = detect_year_from_header(log_path, default_year=folder_year)

            with log_path.open("r", errors="replace") as f:
                for lineno, line in enumerate(f, start=1):
                    if line.startswith(">>>>>>>"):
                        continue

                    gl = parse_glog_line(line, year=file_year)
                    if not gl:
                        continue

                    ts_cand = pd.Timestamp(gl.ts)
                    if not is_timestamp(ts_cand):
                        continue

                    ts: pd.Timestamp = ts_cand
                    on_line(
                        ParsedLine(
                            run=run_id,
                            node=node,
                            log_path=log_path,
                            lineno=lineno,
                            ts=ts,
                            tid=gl.tid,
                            msg=gl.msg,
                        )
                    )
