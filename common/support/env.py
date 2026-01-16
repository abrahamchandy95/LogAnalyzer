from pathlib import Path

from dotenv import dotenv_values

from common.model.config import CompareConfig, AppConfig
from common.model.types import RunInput


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


def load_env_config(*, env_path: Path) -> AppConfig:
    """
    Loads config from .env and enforces that all configured paths are absolute.
    """
    values = dotenv_values(env_path) if env_path.exists() else {}
    if not values:
        raise ValueError(f"Missing or empty env file: {env_path}")

    # Parse Output Directory
    out_dir = _require_abs_path("OUT_DIR", values.get("OUT_DIR"))

    # Parse Run 1 (Use 'or' to handle None types safely)
    run1_key = values.get("RUN_1_KEY") or "run1"
    run1_dir = _require_abs_path("RUN_1_DIR", values.get("RUN_1_DIR"))

    # Parse Run 2
    run2_key = values.get("RUN_2_KEY") or "run2"
    run2_dir = _require_abs_path("RUN_2_DIR", values.get("RUN_2_DIR"))

    # Parse Nodes
    nodes = _parse_nodes(values.get("NODES"))

    # Parse Queries
    base_query = values.get("BASE_QUERY")
    opt_query = values.get("OPT_QUERY")

    if not base_query or not opt_query:
        raise ValueError("Missing BASE_QUERY or OPT_QUERY in .env")

    # Parse Plotting option
    open_plot = _parse_bool(values.get("OPEN_PLOT"), default=False)

    # Construct Config Object
    cfg = CompareConfig(
        runs=(
            RunInput(id=run1_key, path=run1_dir),
            RunInput(id=run2_key, path=run2_dir),
        ),
        nodes=nodes,
        base_query=base_query,
        opt_query=opt_query,
        out_dir=out_dir,
    )

    return AppConfig(cfg=cfg, open_plot=open_plot)
