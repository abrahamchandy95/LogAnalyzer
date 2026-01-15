import os
import subprocess
import sys
from pathlib import Path

from common.reporting import NullReporter, Reporter


def open_file(path: Path, *, reporter: Reporter | None = None) -> None:
    """
    Open a file using the system default application.
    No exception escapes; failures are reported via reporter.
    """
    rep: Reporter = reporter if reporter is not None else NullReporter()

    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
            return

        if os.name == "nt":
            # os.startfile exists only on Windows
            getattr(os, "startfile")(str(path))  # type: ignore[misc]
            return

        subprocess.Popen(["xdg-open", str(path)])
    except Exception as e:
        rep.info(f"WARNING: failed to open file: {path} ({e})")
