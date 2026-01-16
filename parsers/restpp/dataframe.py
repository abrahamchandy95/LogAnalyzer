from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from common.model.constants import RESTPP_GLOB
from common.model.types import RequestId, RunId
from parsers._walker import ParsedLine, LogWalker, walk_logs

from .decode import classify_msg, make_raw_row, make_return_row
from .records import (
    OUT_COLS,
    RestppInfoRecord,
    RestppRawRecord,
    RestppReturnRecord,
    RestppRow,
)


def _first_str(s: pd.Series) -> str | None:
    for x in s:
        if isinstance(x, str):
            return x
    return None


def aggregate_events(
    rows: list[RestppRow], reqinfo: dict[RequestId, dict[str, str]]
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=OUT_COLS)

    df = pd.DataFrame(rows)

    if reqinfo:
        info_df = pd.DataFrame([{"request_id": k, **v} for k, v in reqinfo.items()])
        if not info_df.empty:
            df = df.merge(info_df, on="request_id", how="left")

    agg = df.groupby(["run", "request_id"], as_index=False).agg(
        restpp_ts=("ts", "min"),
        restpp_node=("node", "first"),
        endpoint=("endpoint", _first_str),
        query_name=("query_name", _first_str),
        graph_name=("graph_name", _first_str),
        restpp_return_ms=("restpp_return_ms", "max"),
        restpp_engine=("restpp_engine", _first_str),
        restpp_return_ts=("return_ts", "max"),
    )

    agg = agg.reindex(columns=OUT_COLS)
    agg = agg.set_index(["run", "restpp_ts"]).sort_index().reset_index()
    return agg.reset_index(drop=True)


@dataclass(slots=True)
class RestppCollector:
    rows: list[RestppRow] = field(default_factory=list)
    reqinfo: dict[RequestId, dict[str, str]] = field(default_factory=dict)

    def on_line(self, pl: ParsedLine) -> None:
        rec = classify_msg(pl.msg)
        if rec is None:
            return

        match rec:
            case RestppRawRecord(parsed=raw):
                self.rows.append(make_raw_row(pl=pl, parsed=raw))
            case RestppReturnRecord(parsed=rr):
                self.rows.append(make_return_row(pl=pl, parsed=rr))
            case RestppInfoRecord(parsed=info):
                if info.kv:
                    self.reqinfo.setdefault(info.request_id, {}).update(info.kv)

    def finalize(self) -> pd.DataFrame:
        return aggregate_events(self.rows, self.reqinfo)


def parse_restpp(
    run_id: RunId,
    run_dir: Path,
    *,
    nodes: tuple[str, ...],
    walker: LogWalker = walk_logs,
) -> pd.DataFrame:
    collector = RestppCollector()

    walker(
        run_id=run_id,
        run_dir=run_dir,
        nodes=nodes,
        file_glob=RESTPP_GLOB,
        on_line=collector.on_line,
    )

    return collector.finalize()
