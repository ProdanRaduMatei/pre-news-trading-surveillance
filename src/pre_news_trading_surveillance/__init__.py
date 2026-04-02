"""Pre-News Trading Surveillance."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

__all__ = ["__version__"]

__version__ = "0.1.0"


def _configure_matplotlib_cache() -> None:
    if os.getenv("MPLCONFIGDIR"):
        return
    home_cache = Path.home() / ".matplotlib"
    if home_cache.exists() and os.access(home_cache, os.W_OK):
        return
    fallback = Path(tempfile.gettempdir()) / "pnts-mpl-cache"
    fallback.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(fallback)


_configure_matplotlib_cache()
