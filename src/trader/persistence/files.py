"""Small durable JSON files (OMS snapshot, strategy state): atomic replace, 0600, fsync'd. Blocking IO:
call from ``asyncio.to_thread`` on the event loop."""

from __future__ import annotations

import os
from pathlib import Path


def write_atomic(path: Path, text: str) -> None:
    """Write ``text`` so a crash leaves either the old or the new file, never a torn one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def read_text(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.exists() else None
