
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from .config import PROJECT_ROOT


def load_dotenv(path: str | Path | None = None, override: bool = False) -> list[str]:
    
    path = Path(path) if path else PROJECT_ROOT / ".env"
    if not path.exists():
        return []
    loaded: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value
            loaded.append(key)
    return loaded

# it will show the secret like 123...abc or greater or less than 12 ***
def mask(secret: str | None) -> str:
    if not secret:
        return "<not set>"
    return f"{secret[:4]}...{secret[-4:]}" if len(secret) > 12 else "****"

# for the logging here 
def setup_logging(level: str | int = "INFO") -> None:
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s", "%H:%M:%S"))
    root.addHandler(handler)
    root.setLevel(level)
