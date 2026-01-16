from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from common.model.constants import GPE_GLOB
from common.model.types import Node, RunId
from parsers._walker import ParsedLine, LogWalker, walk_logs

from .decode import (
    classify_msg,
    extract_rid,
    make_step_row,
    make_udf_start_row,
    make_udf_stop_row,
)
from .records import (
    OUT_COLS,
    GPE_DEDUPE_SUBSET,
    GpeRow,
    GpeStepRecord,
    GpeUdfStartRecord,
    GpeUdfStopRecord,
)


def dedupe_gpe_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df2 = df.sort_values(
        ["run", "node", "tid", "ts", "log_path", "lineno"],
        kind="mergesort",
        na_position="last",
    )
    return df2.drop_duplicates(subset=GPE_DEDUPE_SUBSET, keep="first")


@dataclass(slots=True)
class GpeCollector:
    rows: list[GpeRow] = field(default_factory=list)

    def on_line(self, pl: ParsedLine) -> None:
        rec = classify_msg(pl.msg)
        if rec is None:
            return

        rid = extract_rid(pl.msg)

        match rec:
            case GpeStepRecord(parsed=step):
                self.rows.append(make_step_row(pl=pl, request_id=rid, parsed=step))
            case GpeUdfStartRecord(parsed=start):
                self.rows.append(
                    make_udf_start_row(pl=pl, request_id=rid, parsed=start)
                )
            case GpeUdfStopRecord(parsed=stop):
                self.rows.append(make_udf_stop_row(pl=pl, request_id=rid, parsed=stop))

    def finalize(self) -> pd.DataFrame:
        if not self.rows:
            return pd.DataFrame(columns=OUT_COLS)

        df = pd.DataFrame(self.rows).reindex(columns=OUT_COLS)
        df = dedupe_gpe_events(df)
        df = df.set_index(["run", "node", "tid", "ts"]).sort_index().reset_index()
        return df.reset_index(drop=True)


def parse_gpe(
    run_key: RunId,
    run_dir: Path,
    *,
    nodes: tuple[Node, ...],
    walker: LogWalker = walk_logs,
) -> pd.DataFrame:
    collector = GpeCollector()

    walker(
        run_id=run_key,
        run_dir=run_dir,
        nodes=nodes,
        file_glob=GPE_GLOB,
        on_line=collector.on_line,
    )

    return collector.finalize()
