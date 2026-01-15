#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PYTHON="${PYTHON:-python3}"
VENV_DIR=".venv"
HASH_FILE="${VENV_DIR}/.deps_hash"

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
fi

echo "Repo: ${REPO_ROOT}"
echo "System Python: $(${PYTHON} --version)"

# Create venv if missing
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating venv at ${VENV_DIR}..."
  "${PYTHON}" -m venv "${VENV_DIR}"
fi

# Activate venv
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "Venv Python: $(python --version)"

# Compute a deps signature. If you add a lock file later, include it here.
deps_sig="$(python - <<'PY'
import hashlib
from pathlib import Path

h = hashlib.sha256()
for name in ("pyproject.toml",):
    p = Path(name)
    if p.exists():
        h.update(p.read_bytes())
print(h.hexdigest())
PY
)"

need_install=1
if [[ "${FORCE}" == "0" && -f "${HASH_FILE}" ]]; then
  old_sig="$(cat "${HASH_FILE}" || true)"
  if [[ "${old_sig}" == "${deps_sig}" ]]; then
    need_install=0
  fi
fi

if [[ "${need_install}" == "1" ]]; then
  echo "Installing (or updating) dependencies + dev tools..."
  python -m pip install --upgrade pip
  # Install EVERYTHING: runtime deps + dev extras
  python -m pip install -e ".[dev]"
  echo "${deps_sig}" > "${HASH_FILE}"
else
  echo "Dependencies unchanged (pyproject.toml). Skipping pip install."
fi

echo
echo "loganalyzer path: $(command -v loganalyzer || true)"
echo "Verifying command..."
loganalyzer --help >/dev/null
echo "OK: loganalyzer is installed and runnable."
echo "Tip: re-run with --force to reinstall."
