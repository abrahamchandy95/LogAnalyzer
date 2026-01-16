from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from common.model.constants import GPE_GLOB
from common.model.types import Node, RunId
from parsers._walker import ParsedLine, LogWalker, walk_logs
from parsers.dfutils import stable_dedupe

from .decode import DecodedGpe, decode_msg
from .records import OUT_COLS, GPE_DEDUPE_SUBSET, GpeRow
from .rows import row_from_decoded


type GpeDecoder = Callable[[str], DecodedGpe | None]


def dedupe_gpe(df: pd.DataFrame) -> pd.DataFrame:
    return stable_dedupe(
        df,
        sort_cols=["run", "node", "tid", "ts", "log_path", "lineno"],
        subset=GPE_DEDUPE_SUBSET,
    )


@dataclass(slots=True)
class GpeCollector:
    decoder: GpeDecoder
    rows: list[GpeRow] = field(default_factory=list)

    def on_line(self, pl: ParsedLine) -> None:
        dec = self.decoder(pl.msg)
        if dec is None:
            return
        self.rows.append(row_from_decoded(pl, dec))

    def finalize(self) -> pd.DataFrame:
        if not self.rows:
            return pd.DataFrame(columns=OUT_COLS)

        df = pd.DataFrame(self.rows).reindex(columns=OUT_COLS)
        df = dedupe_gpe(df)
        df = df.set_index(["run", "node", "tid", "ts"]).sort_index().reset_index()
        return df.reset_index(drop=True)


def parse_gpe(
    run_key: RunId,
    run_dir: Path,
    *,
    nodes: tuple[Node, ...],
    walker: LogWalker = walk_logs,
    decoder: GpeDecoder = decode_msg,
) -> pd.DataFrame:
    collector = GpeCollector(decoder=decoder)

    walker(
        run_id=run_key,
        run_dir=run_dir,
        nodes=nodes,
        file_glob=GPE_GLOB,
        on_line=collector.on_line,
    )

    return collector.finalize()
