"""Single-writer lock: two pipeline runs must never write the warehouse at once
(e.g. a cron run overlapping a manual run). Uses an atomic create of a lock
file; a lock older than `timeout` is treated as left behind by a crashed run."""
from __future__ import annotations

import json
import logging
import os
import socket
import time
from pathlib import Path
from types import TracebackType

log = logging.getLogger(__name__)


class PipelineLockedError(RuntimeError):
    """Another run holds the lock."""


class RunLock:
    def __init__(self, path: Path, timeout_seconds: float = 7200, owner: str = "pipeline") -> None:
        self.path = Path(path)
        self.timeout = timeout_seconds
        self.owner = owner
        self._held = False

    def _info(self) -> dict:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def acquire(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"pid": os.getpid(), "host": socket.gethostname(),
                              "owner": self.owner, "since": time.time()})
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                info = self._info()
                age = time.time() - float(info.get("since", 0) or 0)
                if age > self.timeout:
                    log.warning("removing stale lock (%.0f min old, pid %s)", age / 60, info.get("pid"))
                    self.path.unlink(missing_ok=True)
                    continue
                raise PipelineLockedError(
                    f"another run is in progress ({info.get('owner', '?')} pid {info.get('pid', '?')} on "
                    f"{info.get('host', '?')}, started {age:.0f}s ago). If it crashed, delete {self.path} "
                    f"or wait {self.timeout / 60:.0f} minutes for it to expire.") from None
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
            self._held = True
            return self
        raise PipelineLockedError(f"could not acquire {self.path}")

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False

    def __enter__(self) -> "RunLock":
        return self.acquire()

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.release()
