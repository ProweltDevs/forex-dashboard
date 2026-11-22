"""Write-ahead buffer for database outages (SPEC §12): records that could not be written are appended to a
local JSONL file (fsync'd), and replayed in order once the database is back. Replaying is idempotent (every
record has a unique ``record_id``), so a crash during a replay only means replaying again.

Blocking IO: call from a worker thread."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path

from trader.domain.time import from_epoch_ms
from trader.observability.logging import get_logger

from .serialize import Record

_log = get_logger("trader.persistence.wal")


def record_from_wal(d: dict[str, object]) -> Record:
    payload = d["payload"]
    assert isinstance(payload, dict)
    account = d.get("account_id")
    return Record(
        record_id=str(d["record_id"]),
        kind=str(d["kind"]),
        account_id=None if account is None else str(account),
        ts=from_epoch_ms(int(str(d["ts_ms"]))),
        payload=payload,
    )


class WalFile:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, records: Sequence[Record]) -> None:
        if not records:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as fh:
            fh.write("".join(json.dumps(r.to_wal(), separators=(",", ":")) + "\n" for r in records))
            fh.flush()
            os.fsync(fh.fileno())

    def read(self) -> list[Record]:
        """All buffered records, oldest first. A torn last line (crash mid-write) is skipped."""
        if not self.path.exists():
            return []
        out = []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                out.append(record_from_wal(json.loads(line)))
            except (ValueError, KeyError, AssertionError):
                if i == len(lines) - 1:
                    _log.warning("wal_torn_line_skipped", path=str(self.path))
                    continue
                raise
        return out

    def count(self) -> int:
        if not self.path.exists():
            return 0
        return sum(1 for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip())

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
