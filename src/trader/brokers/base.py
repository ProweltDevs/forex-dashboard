"""The broker port (docs/SPEC.md §4).

Everything above ``brokers/`` depends on this module and ``trader.domain`` only; adapter internals
(SDK types, JSON payloads, protobufs, error codes) never leak out."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol, runtime_checkable

from trader.domain.enums import OrderType, Timeframe
from trader.domain.events import DomainEvent
from trader.domain.models import (
    AccountState,
    Bar,
    BrokerSnapshot,
    Instrument,
    Order,
    OrderRequest,
    Position,
    Tick,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class Capabilities:
    """What an adapter supports natively. The OMS emulates what is missing; never branch on broker names."""

    order_types: frozenset[OrderType]
    native_sl_tp: bool
    native_trailing_stop: bool
    partial_close: bool
    close_by: bool
    hedging: bool
    netting: bool
    modify_pending: bool
    server_side_expiry: bool
    tick_stream: bool
    candle_history: dict[Timeframe, timedelta]  # how far back each timeframe goes
    max_candles_per_request: int
    sync_order_result: bool  # False = ACK first, the result arrives later via events()
    oco: bool = False
    # True when resubmitting with the same client_order_id can never create a second order (the broker
    # deduplicates). The OMS relies on it to resolve UNKNOWN submits the broker cannot look up.
    idempotent_submit: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerAck:
    """The broker accepted a request for processing. With ``sync_order_result=False`` this says nothing
    about the outcome: that arrives as events."""

    ts: datetime
    client_order_id: str | None = None
    broker_order_id: str | None = None
    position_id: str | None = None
    duplicate: bool = False  # True when the request matched an existing client_order_id (idempotent replay)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderChanges:
    """Changes to a pending order. ``None`` = unchanged; ``remove_sl``/``remove_tp`` clear SL/TP."""

    volume: Decimal | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    sl: Decimal | None = None
    tp: Decimal | None = None
    expiry: datetime | None = None
    remove_sl: bool = False
    remove_tp: bool = False

    def __post_init__(self) -> None:
        if self.remove_sl and self.sl is not None:
            raise ValueError("set either sl or remove_sl, not both")
        if self.remove_tp and self.tp is not None:
            raise ValueError("set either tp or remove_tp, not both")

    @property
    def is_empty(self) -> bool:
        return (
            all(
                v is None
                for v in (self.volume, self.limit_price, self.stop_price, self.sl, self.tp, self.expiry)
            )
            and not self.remove_sl
            and not self.remove_tp
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class RateLimit:
    """A token bucket: ``rate_per_s`` sustained, ``burst`` capacity. ``name`` is the endpoint class."""

    name: str
    rate_per_s: Decimal
    burst: int

    def __post_init__(self) -> None:
        if self.rate_per_s <= 0 or self.burst <= 0:
            raise ValueError("rate_per_s and burst must be > 0")


@dataclass(frozen=True, slots=True, kw_only=True)
class RateLimits:
    buckets: tuple[RateLimit, ...]

    def get(self, name: str) -> RateLimit | None:
        return next((b for b in self.buckets if b.name == name), None)


@runtime_checkable
class Broker(Protocol):
    """One broker account. Instances are owned by a single ``AccountRuntime``."""

    @property
    def name(self) -> str: ...

    @property
    def capabilities(self) -> Capabilities: ...

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def instruments(self) -> list[Instrument]: ...
    async def account(self) -> AccountState: ...
    async def positions(self) -> list[Position]: ...
    async def open_orders(self) -> list[Order]: ...
    async def snapshot(self) -> BrokerSnapshot: ...
    async def find_order(self, client_order_id: str, since: datetime) -> Order | None:
        """Targeted reconcile: the broker's view of an order we sent (open or finished since ``since``),
        or None if the broker has no such order. Used to resolve UNKNOWN submits after timeouts/restarts."""
        ...

    async def place_order(self, req: OrderRequest) -> BrokerAck: ...
    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck: ...
    async def cancel_order(self, order_id: str) -> BrokerAck: ...
    async def modify_position(
        self, position_id: str, sl: Decimal | None, tp: Decimal | None
    ) -> BrokerAck: ...
    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck: ...
    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]: ...
    async def subscribe_ticks(self, symbols: Sequence[str]) -> None: ...
    def events(self) -> AsyncIterator[DomainEvent]: ...
    async def limits(self) -> RateLimits: ...


@runtime_checkable
class MarketDataSource(Protocol):
    """Market data only (no trading). Feeds the paper broker: a real adapter in paper mode, or a fake/replay
    source in tests and, later, the backtester."""

    async def instruments(self) -> list[Instrument]: ...
    async def subscribe(self, symbols: Sequence[str]) -> None: ...
    def ticks(self) -> AsyncIterator[Tick]: ...
    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]: ...
