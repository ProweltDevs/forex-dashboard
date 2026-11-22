"""Generic JSON encoding for persisted records: dataclasses (domain events, models), Decimal -> str,
datetime -> epoch ms (the storage rule), date -> ISO, timedelta -> ms, enums -> value."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum

from trader.domain.ids import new_client_order_id
from trader.domain.jsontypes import JsonObj, JsonValue
from trader.domain.time import to_epoch_ms


def to_json(value: object) -> JsonValue:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return to_json(value.value)
    if isinstance(value, datetime):
        return to_epoch_ms(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1000)
    if isinstance(value, float):
        return str(value)  # floats never in domain money; kept exact as text
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: to_json(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(k): to_json(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [to_json(v) for v in value]
    raise TypeError(f"cannot persist {type(value).__name__}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Record:
    """One thing to persist. ``record_id`` (ULID) is the idempotency key: replaying a record after an
    outage, or twice, changes nothing."""

    record_id: str
    kind: str  # the event class name, or a named record ("trade", "audit", "strategy_state", ...)
    account_id: str | None
    ts: datetime
    payload: JsonObj

    def to_wal(self) -> JsonObj:
        return {
            "record_id": self.record_id,
            "kind": self.kind,
            "account_id": self.account_id,
            "ts_ms": to_epoch_ms(self.ts),
            "payload": self.payload,
        }


def record(kind: str, account_id: str | None, ts: datetime, payload: object) -> Record:
    data = to_json(payload)
    if not isinstance(data, dict):
        raise TypeError(f"{kind} payload must encode to an object")
    return Record(record_id=new_client_order_id(), kind=kind, account_id=account_id, ts=ts, payload=data)


def event_record(event: object, account_id: str | None, ts: datetime) -> Record:
    """A domain event as a record (kind = its class name)."""
    return record(type(event).__name__, account_id, ts, event)
