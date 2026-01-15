from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

from common.config import CompareConfig
from common.types import RunInput


@dataclass(frozen=True, slots=True)
class EnvConfig:
    cfg: CompareConfig
    open_plot: bool


def _require_abs_path(var: str, raw: str | None) -> Path:
    if not raw:
        raise ValueError(f"Missing {var} in .env")
    p = Path(raw)
    if not p.is_absolute():
        raise ValueError(f"{var} must be an absolute path. Got: {raw}")
    return p


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_nodes(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ("m1", "m2", "m3", "m4")
    return tuple(x for x in raw.split() if x)


def load_env_config(*, env_path: Path) -> EnvConfig:
    """
    Loads config from .env and enforces that all configured paths are absolute.
    """
    values = dotenv_values(env_path) if env_path.exists() else {}
    if not values:
        raise ValueError(f"Missing or empty env file: {env_path}")

    # 1. Parse Output Directory
    out_dir = _require_abs_path("OUT_DIR", values.get("OUT_DIR"))

    # 2. Parse Run 1 (Use 'or' to handle None types safely)
    run1_key = values.get("RUN_1_KEY") or "run1"
    run1_dir = _require_abs_path("RUN_1_DIR", values.get("RUN_1_DIR"))

    # 3. Parse Run 2
    run2_key = values.get("RUN_2_KEY") or "run2"
    run2_dir = _require_abs_path("RUN_2_DIR", values.get("RUN_2_DIR"))

    # 4. Parse Nodes
    nodes = _parse_nodes(values.get("NODES"))

    # 5. Parse Queries
    base_query = values.get("BASE_QUERY")
    opt_query = values.get("OPT_QUERY")

    # Type Guard: Ensure queries are strings, not None
    if not base_query or not opt_query:
        raise ValueError("Missing BASE_QUERY or OPT_QUERY in .env")

    # 6. Parse Plotting option
    open_plot = _parse_bool(values.get("OPEN_PLOT"), default=False)

    # 7. Construct Config Object
    # All variables are now pre-calculated above, fixing the syntax error
    cfg = CompareConfig(
        runs=(
            RunInput(key=run1_key, path=run1_dir),
            RunInput(key=run2_key, path=run2_dir),
        ),
        nodes=nodes,
        base_query=base_query,
        opt_query=opt_query,
        out_dir=out_dir,
    )

    return EnvConfig(cfg=cfg, open_plot=open_plot)
