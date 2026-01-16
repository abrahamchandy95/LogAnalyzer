from dataclasses import dataclass
from pathlib import Path

from common.model.types import RunInput, QueryName


@dataclass(frozen=True, slots=True)
class CompareConfig:
    runs: tuple[RunInput, ...]
    nodes: tuple[str, ...]
    base_query: QueryName
    opt_query: QueryName
    out_dir: Path = Path("out")


@dataclass(frozen=True, slots=True)
class AppConfig:
    cfg: CompareConfig
    open_plot: bool
