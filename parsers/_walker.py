from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeIs
from datetime import datetime

import pandas as pd
from pandas._libs import NaTType

from common.parse.time import infer_year_from_any_line_epoch
from common.parse.glog import parse_glog_line

from common.model.types import RunId, Node

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
    default_year = datetime.now().year

    for node in nodes:
        node_dir = run_dir / node
        if not node_dir.exists():
            continue

        for log_path in node_dir.glob(file_glob):
            if not log_path.is_file():
                continue

            file_year = infer_year_from_any_line_epoch(
                log_path, default_year=default_year
            )

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
