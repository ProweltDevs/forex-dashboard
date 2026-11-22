"""Typed domain events published on the in-process event bus."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .enums import ConnectionState, OrderStatus, PositionChange, RiskDecision
from .models import AccountState, Bar, Fill, Order, Position, Tick


@dataclass(frozen=True, slots=True, kw_only=True)
class TickEvent:
    tick: Tick
    broker: str

    @property
    def ts(self) -> datetime:
        return self.tick.ts


@dataclass(frozen=True, slots=True, kw_only=True)
class BarEvent:
    bar: Bar
    broker: str

    @property
    def ts(self) -> datetime:
        return self.bar.start


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderEvent:
    account_id: str
    order: Order
    previous_status: OrderStatus | None
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class FillEvent:
    account_id: str
    fill: Fill
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionEvent:
    account_id: str
    position: Position
    change: PositionChange
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountEvent:
    account_id: str
    state: AccountState
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectionEvent:
    account_id: str
    broker: str
    state: ConnectionState
    ts: datetime
    reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class RiskEvent:
    account_id: str
    decision: RiskDecision
    rule: str
    reason: str
    ts: datetime
    strategy_id: str | None = None
    client_order_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StrategyEvent:
    strategy_id: str
    kind: str  # e.g. "started", "paused", "error", "slow_callback"
    message: str
    ts: datetime


type DomainEvent = (
    TickEvent
    | BarEvent
    | OrderEvent
    | FillEvent
    | PositionEvent
    | AccountEvent
    | ConnectionEvent
    | RiskEvent
    | StrategyEvent
)
