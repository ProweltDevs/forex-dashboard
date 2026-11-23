"""Persistence of OMS state so a restart can resume exactly (SPEC §6, §15: "never leave orders in
PENDING_SUBMIT unrecorded").

Phase 5 ships a JSON file store (atomic replace, 0600) and an in-memory store; phase 9 adds Postgres
behind the same protocol. The state is small: open orders, recent finished orders, strategy attribution,
processed fill ids and emulations.
"""

from __future__ import annotations

import fcntl
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from trader.domain.errors import ConfigError
from trader.domain.jsontypes import JsonObj, JsonValue
from trader.persistence.files import read_text, write_atomic

SCHEMA = "oms/1"


@dataclass(slots=True)
class OmsState:
    orders: list[JsonObj] = field(default_factory=list)  # order_to_json + {"attempts", "first_sent_ms"}
    position_strategy: dict[str, str] = field(default_factory=dict)
    fill_ids: list[str] = field(default_factory=list)
    emulations: list[JsonObj] = field(default_factory=list)
    positions: list[JsonObj] = field(default_factory=list)
    risk: JsonObj = field(default_factory=dict)  # RiskGate.dump(): kill switch, day start, equity peak

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "schema": SCHEMA,
            "orders": list(self.orders),
            "position_strategy": dict(self.position_strategy),
            "fill_ids": list(self.fill_ids),
            "emulations": list(self.emulations),
            "positions": list(self.positions),
            "risk": dict(self.risk),
        }

    @classmethod
    def from_json(cls, data: dict[str, JsonValue]) -> OmsState:
        if data.get("schema") != SCHEMA:
            raise ValueError(f"unsupported OMS state schema {data.get('schema')!r}")
        orders = data.get("orders") or []
        fills = data.get("fill_ids") or []
        emus = data.get("emulations") or []
        pos = data.get("positions") or []
        strat = data.get("position_strategy") or {}
        risk = data.get("risk") or {}
        assert isinstance(orders, list)
        assert isinstance(fills, list)
        assert isinstance(emus, list)
        assert isinstance(pos, list)
        assert isinstance(strat, dict)
        assert isinstance(risk, dict)
        return cls(
            orders=[o for o in orders if isinstance(o, dict)],
            position_strategy={str(k): str(v) for k, v in strat.items()},
            fill_ids=[str(f) for f in fills],
            emulations=[e for e in emus if isinstance(e, dict)],
            positions=[p for p in pos if isinstance(p, dict)],
            risk=risk,
        )


class OmsStore(Protocol):
    def open(self) -> None:
        """Take ownership of the account's state (exclusive). Raises ``AccountLocked`` when another live
        process owns it."""
        ...

    def load(self) -> OmsState | None: ...
    def save(self, state: OmsState) -> None: ...
    def close(self) -> None: ...


class AccountLocked(ConfigError):
    """Another running process (``trader run`` or ``trader kill``) manages this account's OMS state."""


class MemoryStore:
    """For tests and paper runs that should not persist. ``save`` round-trips through JSON so tests catch
    anything that would not survive a real restart."""

    def __init__(self) -> None:
        self._raw: str | None = None
        self.saves = 0

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def load(self) -> OmsState | None:
        return None if self._raw is None else OmsState.from_json(json.loads(self._raw))

    def save(self, state: OmsState) -> None:
        self._raw = json.dumps(state.to_json())
        self.saves += 1


class JsonFileStore:
    """One file per account: ``{data_dir}/oms/{account_id}.json``. Writes are atomic (temp + rename).

    ``open()`` takes an exclusive ``flock`` on ``{account_id}.json.lock`` for as long as the OMS runs: one
    writer per account (CLAUDE.md rule 3). A second ``trader run``, or ``trader kill`` while ``trader run``
    manages the account, is refused instead of silently overwriting the other's state (and its kill
    switch). The kernel drops the lock when the process dies, so a crash never leaves a stale lock.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock_path = path.with_name(path.name + ".lock")
        self._lock: int | None = None

    def open(self) -> None:
        if self._lock is not None:
            return
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            owner = os.read(fd, 64).decode(errors="replace").strip() or "?"
            os.close(fd)
            raise AccountLocked(
                f"{self.path.stem}: another process (pid {owner}) manages this account. Use the running "
                "process's API (POST /risk/kill) or Telegram (/kill) instead, or stop it first."
            ) from None
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        self._lock = fd

    def close(self) -> None:
        if self._lock is not None:
            fcntl.flock(self._lock, fcntl.LOCK_UN)
            os.close(self._lock)
            self._lock = None

    def load(self) -> OmsState | None:
        raw = read_text(self.path)
        return None if raw is None else OmsState.from_json(json.loads(raw))

    def save(self, state: OmsState) -> None:
        write_atomic(self.path, json.dumps(state.to_json(), separators=(",", ":")))
