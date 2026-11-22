"""Audit log of manual commands (SPEC §12, §13): who did what, when, and the result.

Every entry goes to a local JSONL file (``data/audit/audit.jsonl``, 0600, fsync'd per entry), so the trail
exists without a database, and to ``audit_log`` in PostgreSQL when persistence is configured. ``recent()``
reads the local file: it holds every entry this data directory has seen.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from trader.domain.jsontypes import JsonObj
from trader.domain.time import Clock, from_epoch_ms, to_epoch_ms

from .serialize import to_json

if TYPE_CHECKING:
    from .service import Persistence


def audit_path(data_dir: Path) -> Path:
    return data_dir / "audit" / "audit.jsonl"


@dataclass(frozen=True, slots=True, kw_only=True)
class AuditEntry:
    ts: datetime
    actor: str  # "api", "cli", "telegram"
    action: str  # e.g. order_submit, kill, strategy_pause
    account_id: str | None
    details: JsonObj
    result: str

    def to_json(self) -> JsonObj:
        return {
            "ts_ms": to_epoch_ms(self.ts),
            "actor": self.actor,
            "action": self.action,
            "account_id": self.account_id,
            "details": self.details,
            "result": self.result,
        }

    @classmethod
    def from_json(cls, d: JsonObj) -> AuditEntry:
        details = d.get("details")
        account = d.get("account_id")
        return cls(
            ts=from_epoch_ms(int(str(d["ts_ms"]))),
            actor=str(d["actor"]),
            action=str(d["action"]),
            account_id=None if account is None else str(account),
            details=details if isinstance(details, dict) else {},
            result=str(d["result"]),
        )


def _append(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _read(path: Path) -> list[AuditEntry]:
    if not path.exists():
        return []
    out: list[AuditEntry] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:  # a torn last line from a crash mid-write
            continue
        if isinstance(raw, dict):
            out.append(AuditEntry.from_json(raw))
    return out


class AuditLog:
    def __init__(self, path: Path, clock: Clock, persistence: Persistence | None = None) -> None:
        self.path = path
        self._clock = clock
        self._persistence = persistence
        self._lock = asyncio.Lock()

    async def record(
        self,
        *,
        actor: str,
        action: str,
        account_id: str | None,
        details: Mapping[str, object],
        result: str,
    ) -> AuditEntry:
        data = to_json(dict(details))
        entry = AuditEntry(
            ts=self._clock.now(),
            actor=actor,
            action=action,
            account_id=account_id,
            details=data if isinstance(data, dict) else {},
            result=result,
        )
        async with self._lock:  # one writer: lines never interleave
            await asyncio.to_thread(_append, self.path, json.dumps(entry.to_json(), separators=(",", ":")))
        if self._persistence is not None:
            self._persistence.put(
                "audit",
                account_id,
                {"actor": actor, "action": action, "details": entry.details, "result": result},
            )
        return entry

    async def recent(
        self, *, limit: int = 100, action: str | None = None, account_id: str | None = None
    ) -> list[AuditEntry]:
        """Newest first."""
        entries = await asyncio.to_thread(_read, self.path)
        picked = [
            e
            for e in reversed(entries)
            if (action is None or e.action == action) and (account_id is None or e.account_id == account_id)
        ]
        return picked[:limit]
