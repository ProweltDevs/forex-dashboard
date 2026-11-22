"""Canonical domain models. Frozen, slotted dataclasses with Decimal money/price/volume fields and
timezone-aware UTC datetimes. No broker, IO or framework imports here."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time
from decimal import Decimal

from .enums import OrderStatus, OrderType, Side, Timeframe, TimeInForce
from .errors import InvalidPrice, InvalidVolume
from .money import (
    ZERO,
    floor_to_step,
    is_multiple_of,
    require_decimal,
    require_non_negative,
    require_positive,
    round_to_step,
)
from .time import ensure_utc


def _require_utc(name: str, value: datetime | None) -> None:
    if value is not None:
        try:
            ensure_utc(value)
        except ValueError as exc:
            raise ValueError(f"{name}: {exc}") from exc


# --------------------------------------------------------------------------------------- instruments


@dataclass(frozen=True, slots=True, kw_only=True)
class TradingSession:
    """A weekly trading window in UTC. ``weekday`` follows ``datetime.weekday()`` (Mon=0).
    ``end`` <= ``start`` means the window crosses midnight into the next day."""

    weekday: int
    start: time
    end: time

    def __post_init__(self) -> None:
        if not 0 <= self.weekday <= 6:
            raise ValueError(f"weekday must be 0..6, got {self.weekday}")
        if self.start.tzinfo is not None or self.end.tzinfo is not None:
            raise ValueError("session times are plain UTC wall times; do not attach tzinfo")


@dataclass(frozen=True, slots=True, kw_only=True)
class Instrument:
    symbol: str  # canonical, e.g. "EURUSD"
    broker_symbol: str  # e.g. "EURUSDm" (Exness) or "EURUSD" (cTrader)
    broker_id: str  # broker's instrument ID (cTrader numeric symbolId as str)
    digits: int
    tick_size: Decimal
    contract_size: Decimal  # units per 1 lot
    min_volume: Decimal  # lots
    max_volume: Decimal  # lots
    volume_step: Decimal  # lots
    base_currency: str
    quote_currency: str
    margin_currency: str
    hedging_allowed: bool = True
    sessions: tuple[TradingSession, ...] = ()

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if self.digits < 0:
            raise ValueError(f"digits must be >= 0, got {self.digits}")
        for name in ("tick_size", "contract_size", "min_volume", "max_volume", "volume_step"):
            require_positive(name, getattr(self, name))
        if self.min_volume > self.max_volume:
            raise ValueError(f"min_volume {self.min_volume} > max_volume {self.max_volume}")

    def normalize_volume(self, lots: Decimal) -> Decimal:
        """Cap at max, round DOWN to the volume step (never up). Raise if below the minimum."""
        require_decimal("lots", lots)
        capped = min(lots, self.max_volume)
        rounded = floor_to_step(capped, self.volume_step)
        if rounded < self.min_volume:
            raise InvalidVolume(f"{self.symbol}: {lots} lots is below min volume {self.min_volume}")
        return rounded

    def is_valid_volume(self, lots: Decimal) -> bool:
        return self.min_volume <= lots <= self.max_volume and is_multiple_of(lots, self.volume_step)

    def round_price(self, price: Decimal) -> Decimal:
        """Nearest valid price on the tick grid, quantized to ``digits``."""
        require_decimal("price", price)
        return round_to_step(price, self.tick_size).quantize(Decimal(1).scaleb(-self.digits))

    def check_price(self, price: Decimal) -> None:
        if price <= 0 or not is_multiple_of(price, self.tick_size):
            raise InvalidPrice(f"{self.symbol}: {price} is not on the tick grid {self.tick_size}")

    def is_open(self, ts: datetime) -> bool:
        """True if ``ts`` falls inside a trading session (no sessions configured = always open)."""
        if not self.sessions:
            return True
        ts = ensure_utc(ts)
        now, weekday = ts.time(), ts.weekday()
        for s in self.sessions:
            if s.start < s.end:
                if s.weekday == weekday and s.start <= now < s.end:
                    return True
            elif (s.weekday == weekday and now >= s.start) or (
                (s.weekday + 1) % 7 == weekday and now < s.end
            ):
                return True  # window crosses midnight
        return False

    def lots_to_units(self, lots: Decimal) -> Decimal:
        return lots * self.contract_size

    def units_to_lots(self, units: Decimal) -> Decimal:
        return units / self.contract_size


# --------------------------------------------------------------------------------------- market data


@dataclass(frozen=True, slots=True, kw_only=True)
class Tick:
    """Best bid/ask. Hot path: no validation beyond types; adapters build valid ticks.

    ``ts`` is the canonical event time (broker time when available), ``broker_ts`` the raw broker
    timestamp, ``recv_ts`` local UTC receive time and ``recv_mono_ns`` the local monotonic clock
    (for latency maths that must not jump with NTP adjustments)."""

    symbol: str
    bid: Decimal
    ask: Decimal
    ts: datetime
    recv_ts: datetime
    recv_mono_ns: int
    broker_ts: datetime | None = None

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid

    def price_for(self, side: Side) -> Decimal:
        """Price a market order on ``side`` would execute at (buy at ask, sell at bid)."""
        return self.ask if side is Side.BUY else self.bid


@dataclass(frozen=True, slots=True, kw_only=True)
class Bar:
    symbol: str
    timeframe: Timeframe
    start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal  # tick volume unless the broker provides real volume
    complete: bool

    def __post_init__(self) -> None:
        _require_utc("start", self.start)
        for name in ("open", "high", "low", "close"):
            require_decimal(name, getattr(self, name))
        require_non_negative("volume", self.volume)
        if not (self.low <= min(self.open, self.close) and self.high >= max(self.open, self.close)):
            raise ValueError(f"inconsistent OHLC for {self.symbol} {self.timeframe} @ {self.start}")


# --------------------------------------------------------------------------------------- orders


def _validate_order_fields(
    *,
    order_type: OrderType,
    volume: Decimal,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    sl: Decimal | None,
    tp: Decimal | None,
    max_slippage: Decimal | None,
    time_in_force: TimeInForce,
    expiry: datetime | None,
) -> None:
    require_positive("volume", volume)
    for name, value in (("limit_price", limit_price), ("stop_price", stop_price), ("sl", sl), ("tp", tp)):
        if value is not None:
            require_positive(name, value)
    if max_slippage is not None:
        require_non_negative("max_slippage", max_slippage)

    needs_limit = order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)
    needs_stop = order_type in (OrderType.STOP, OrderType.STOP_LIMIT)
    if needs_limit != (limit_price is not None):
        raise ValueError(f"{order_type} {'requires' if needs_limit else 'must not have'} limit_price")
    if needs_stop != (stop_price is not None):
        raise ValueError(f"{order_type} {'requires' if needs_stop else 'must not have'} stop_price")

    _require_utc("expiry", expiry)
    if (time_in_force is TimeInForce.GTD) != (expiry is not None):
        raise ValueError("expiry is required for GTD and only allowed with GTD")
    if order_type is OrderType.MARKET and time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise ValueError(f"MARKET orders cannot be {time_in_force}")


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderIntent:
    """What a strategy (or a manual command) wants to do. Not yet approved by risk and has no
    client_order_id: the OMS assigns one when it turns the intent into an ``OrderRequest``.

    ``signal_ts`` / ``signal_price`` feed execution-quality analytics (signal -> fill slippage)."""

    symbol: str
    side: Side
    order_type: OrderType
    volume: Decimal  # lots
    strategy_id: str
    strategy_version: str
    signal_ts: datetime
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None  # trigger price for STOP / STOP_LIMIT
    sl: Decimal | None = None
    tp: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    expiry: datetime | None = None
    max_slippage: Decimal | None = None  # in price units
    signal_price: Decimal | None = None
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_order_fields(
            order_type=self.order_type,
            volume=self.volume,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            sl=self.sl,
            tp=self.tp,
            max_slippage=self.max_slippage,
            time_in_force=self.time_in_force,
            expiry=self.expiry,
        )
        _require_utc("signal_ts", self.signal_ts)
        if self.signal_price is not None:
            require_positive("signal_price", self.signal_price)

    def to_request(self, client_order_id: str, *, volume: Decimal | None = None) -> OrderRequest:
        """Build the request the OMS sends. ``volume`` overrides the intent volume (after sizing/rounding)."""
        return OrderRequest(
            client_order_id=client_order_id,
            symbol=self.symbol,
            side=self.side,
            order_type=self.order_type,
            volume=self.volume if volume is None else volume,
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            sl=self.sl,
            tp=self.tp,
            time_in_force=self.time_in_force,
            expiry=self.expiry,
            max_slippage=self.max_slippage,
            signal_ts=self.signal_ts,
            signal_price=self.signal_price,
            tags=self.tags,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRequest:
    """A risk-approved order with its idempotent ``client_order_id``. This is what adapters receive."""

    client_order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    volume: Decimal  # lots
    strategy_id: str
    strategy_version: str
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    sl: Decimal | None = None
    tp: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    expiry: datetime | None = None
    max_slippage: Decimal | None = None
    signal_ts: datetime | None = None
    signal_price: Decimal | None = None
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not 5 <= len(self.client_order_id) <= 128:
            raise ValueError("client_order_id must be 5..128 chars")
        _validate_order_fields(
            order_type=self.order_type,
            volume=self.volume,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            sl=self.sl,
            tp=self.tp,
            max_slippage=self.max_slippage,
            time_in_force=self.time_in_force,
            expiry=self.expiry,
        )
        _require_utc("signal_ts", self.signal_ts)

    @property
    def entry_price(self) -> Decimal | None:
        """The price a pending order works at (limit for LIMIT/STOP_LIMIT, trigger for STOP)."""
        return self.limit_price if self.limit_price is not None else self.stop_price


@dataclass(frozen=True, slots=True, kw_only=True)
class StatusChange:
    status: OrderStatus
    ts: datetime
    reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Order:
    """An order and its full lifecycle. Only the OMS creates new versions (via ``oms.state_machine``)."""

    request: OrderRequest
    status: OrderStatus
    history: tuple[StatusChange, ...]
    broker_order_id: str | None = None
    filled_volume: Decimal = ZERO
    avg_fill_price: Decimal | None = None
    rejection_reason: str | None = None

    def __post_init__(self) -> None:
        require_non_negative("filled_volume", self.filled_volume)
        if self.filled_volume > self.request.volume:
            raise ValueError(f"filled_volume {self.filled_volume} exceeds order volume {self.request.volume}")
        if not self.history or self.history[-1].status is not self.status:
            raise ValueError("history must end with the current status")

    @classmethod
    def new(cls, request: OrderRequest, ts: datetime) -> Order:
        return cls(
            request=request, status=OrderStatus.NEW, history=(StatusChange(status=OrderStatus.NEW, ts=ts),)
        )

    @property
    def client_order_id(self) -> str:
        return self.request.client_order_id

    @property
    def remaining_volume(self) -> Decimal:
        return self.request.volume - self.filled_volume

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def updated_ts(self) -> datetime:
        return self.history[-1].ts

    def with_changes(
        self,
        *,
        broker_order_id: str | None = None,
        filled_volume: Decimal | None = None,
        avg_fill_price: Decimal | None = None,
    ) -> Order:
        return replace(
            self,
            broker_order_id=self.broker_order_id if broker_order_id is None else broker_order_id,
            filled_volume=self.filled_volume if filled_volume is None else filled_volume,
            avg_fill_price=self.avg_fill_price if avg_fill_price is None else avg_fill_price,
        )


# --------------------------------------------------------------------------------------- fills, positions


@dataclass(frozen=True, slots=True, kw_only=True)
class Fill:
    """A single execution (a broker "deal")."""

    fill_id: str  # broker deal ID
    client_order_id: str | None  # None for fills of orders we did not create
    broker_order_id: str
    position_id: str
    symbol: str
    side: Side
    volume: Decimal  # lots
    price: Decimal
    ts: datetime
    commission: Decimal = ZERO  # account currency; negative = cost
    swap: Decimal = ZERO  # account currency
    realized_pnl: Decimal | None = None  # gross, account currency, for closing deals

    def __post_init__(self) -> None:
        require_positive("volume", self.volume)
        require_positive("price", self.price)
        require_decimal("commission", self.commission)
        require_decimal("swap", self.swap)
        if self.realized_pnl is not None:
            require_decimal("realized_pnl", self.realized_pnl)
        _require_utc("ts", self.ts)


@dataclass(frozen=True, slots=True, kw_only=True)
class Position:
    position_id: str
    symbol: str
    side: Side
    volume: Decimal  # lots
    entry_price: Decimal
    opened_ts: datetime
    strategy_id: str  # "manual" for adopted orphans
    sl: Decimal | None = None
    tp: Decimal | None = None
    unrealized_pnl: Decimal = ZERO  # account currency; recomputed by the portfolio on each tick
    swap: Decimal = ZERO
    commission: Decimal = ZERO

    def __post_init__(self) -> None:
        require_positive("volume", self.volume)
        require_positive("entry_price", self.entry_price)
        for name in ("sl", "tp"):
            value = getattr(self, name)
            if value is not None:
                require_positive(name, value)
        _require_utc("opened_ts", self.opened_ts)


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountState:
    account_id: str
    currency: str
    balance: Decimal
    equity: Decimal
    margin: Decimal
    free_margin: Decimal
    leverage: int
    ts: datetime
    margin_level: Decimal | None = None  # percent; None when no margin is used

    def __post_init__(self) -> None:
        for name in ("balance", "equity", "free_margin"):
            require_decimal(name, getattr(self, name))
        require_non_negative("margin", self.margin)
        if self.margin_level is not None:
            require_decimal("margin_level", self.margin_level)
        if self.leverage <= 0:
            raise ValueError(f"leverage must be > 0, got {self.leverage}")
        _require_utc("ts", self.ts)


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerSnapshot:
    """Everything needed to reconcile local state with the broker (the source of truth)."""

    account: AccountState
    positions: tuple[Position, ...]
    orders: tuple[Order, ...]
    ts: datetime
