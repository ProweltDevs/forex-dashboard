"""Persistence of OMS state so a restart can resume exactly (SPEC §6, §15: "never leave orders in
PENDING_SUBMIT unrecorded").

Phase 5 ships a JSON file store (atomic replace, 0600) and an in-memory store; phase 9 adds Postgres
behind the same protocol. The state is small: open orders, recent finished orders, strategy attribution,
processed fill ids and emulations.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from trader.domain.jsontypes import JsonObj, JsonValue

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
    def load(self) -> OmsState | None: ...
    def save(self, state: OmsState) -> None: ...


class MemoryStore:
    """For tests and paper runs that should not persist. ``save`` round-trips through JSON so tests catch
    anything that would not survive a real restart."""

    def __init__(self) -> None:
        self._raw: str | None = None
        self.saves = 0

    def load(self) -> OmsState | None:
        return None if self._raw is None else OmsState.from_json(json.loads(self._raw))

    def save(self, state: OmsState) -> None:
        self._raw = json.dumps(state.to_json())
        self.saves += 1


class JsonFileStore:
    """One file per account: ``{data_dir}/oms/{account_id}.json``. Writes are atomic (temp + rename)."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> OmsState | None:
        if not self.path.exists():
            return None
        return OmsState.from_json(json.loads(self.path.read_text(encoding="utf-8")))

    def save(self, state: OmsState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f".{self.path.name}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state.to_json(), fh, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
