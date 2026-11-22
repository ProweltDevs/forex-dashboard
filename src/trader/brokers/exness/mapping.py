"""Pure conversions between Exness Trader API JSON and domain objects (spec v1.0.2; see
docs/brokers/exness.md).

- Volumes are lots as decimal strings (``volume_min`` 0.01 for EURUSD, ``contract_size`` 100000), the same
  unit as the domain.
- Trading prices and money are decimal strings; market-data prices (ticks, candles) are JSON numbers.
- Instrument names may carry an account-type suffix; ``canonical_symbol`` strips a configured suffix.
- Sessions are weekly ``"Mon, 10:00:00"`` UTC strings with an inclusive end.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from trader.domain.enums import OrderStatus, OrderType, Side, Timeframe, TimeInForce
from trader.domain.errors import (
    AuthExpired,
    BrokerError,
    BrokerUnavailable,
    InsufficientMargin,
    InvalidPrice,
    InvalidVolume,
    MarketClosed,
    OrderNotFound,
    OrderRejected,
    PositionNotFound,
    RateLimited,
    Requote,
    UnknownSymbol,
)
from trader.domain.models import (
    Bar,
    Instrument,
    Order,
    OrderRequest,
    Position,
    StatusChange,
    Tick,
    TradingSession,
)
from trader.domain.time import ensure_utc

type Json = dict[str, Any]

MANUAL_STRATEGY = "manual"

# --------------------------------------------------------------------------------------------- values


def dec(value: object, field: str = "value") -> Decimal:
    """Decimal string (or JSON number for market data) -> Decimal. Floats go through repr to keep digits."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field}: expected a number, got {value!r}")
    try:
        return Decimal(repr(value)) if isinstance(value, float) else Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field}: not a decimal: {value!r}") from exc


def opt_dec(value: object) -> Decimal | None:
    return None if value is None or value == "" else dec(value)


def price_str(value: Decimal, digits: int) -> str:
    """Domain price -> request string with at most ``point_digits`` fractional digits."""
    q = value.quantize(Decimal(1).scaleb(-digits))
    if q != value:
        raise InvalidPrice(f"price {value} has more than {digits} decimals")
    return format(q, "f")


def volume_str(value: Decimal) -> str:
    return format(value.normalize(), "f") if value == value.to_integral_value() else format(value, "f")


def parse_time(value: str) -> datetime:
    """ISO 8601 with explicit offset (``2026-03-16T12:54:30+00:00``) or ``Z``."""
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def iso(value: datetime) -> str:
    """Query-string timestamp. ``Z`` avoids a ``+`` that would need percent-encoding in a signed query."""
    return ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ensure_utc(value).microsecond // 1000:03d}Z"


# --------------------------------------------------------------------------------------------- errors

_ERRORS: dict[str, type[BrokerError]] = {
    "SYSTEM_RATE_LIMIT": RateLimited,
    "REQUEST_RATE_LIMIT": RateLimited,
    "AUTH_RATE_LIMIT": RateLimited,
    "SYSTEM_TEMPORARY_UNAVAILABLE": BrokerUnavailable,
    "SYSTEM_INTERNAL_ERROR": BrokerUnavailable,
    "AUTH_INVALID_API_KEY": AuthExpired,
    "AUTH_INVALID_SIGNATURE": AuthExpired,
    "AUTH_PERMISSION_DENIED": AuthExpired,
    "ACCOUNT_DISABLED": OrderRejected,
    "ACCOUNT_CLOSE_ONLY": OrderRejected,
    "REQUEST_INVALID_PRICE": InvalidPrice,
    "REQUEST_ORDER_NOT_FOUND": OrderNotFound,
    "REQUEST_POSITION_NOT_FOUND": PositionNotFound,
    "MARKET_INSTRUMENT_NOT_FOUND": UnknownSymbol,
    "MARKET_TRADE_DISABLED": MarketClosed,
    "MARKET_CLOSE_ONLY": MarketClosed,
    "MARKET_SESSION_CLOSED": MarketClosed,
    "MARKET_NO_QUOTES": MarketClosed,
    "TRADING_RULE_INVALID_VOLUME": InvalidVolume,
    "TRADING_RULE_VOLUME_TOO_SMALL": InvalidVolume,
    "TRADING_RULE_VOLUME_TOO_LARGE": InvalidVolume,
    "TRADING_RULE_INVALID_VOLUME_STEP": InvalidVolume,
    "TRADING_RULE_INVALID_REMAINING_VOLUME": InvalidVolume,
    "TRADING_RULE_INVALID_PRICE_LEVELS": InvalidPrice,
    "TRADING_RULE_INSUFFICIENT_MARGIN": InsufficientMargin,
    "TRADING_RULE_MARGIN_LEVEL_TOO_LOW": InsufficientMargin,
    "TRADING_RULE_OPERATION_RESTRICTED": OrderRejected,
    "TRADING_RULE_TOO_MANY_PENDING_ORDERS": OrderRejected,
    "TRADING_RULE_TOO_MANY_OPEN_POSITIONS": OrderRejected,
    "EXECUTION_REQUOTE": Requote,
    "EXECUTION_REJECTED": OrderRejected,
}


def map_error(code_name: str, *, retry_after_ms: int | None = None) -> BrokerError:
    """Exness ``error_message`` (``CATEGORY_REASON``) -> canonical error; unknown -> ``BrokerError``."""
    cls = _ERRORS.get(code_name, BrokerError)
    if cls is RateLimited:
        return RateLimited(code_name, broker_code=code_name, retry_after_ms=retry_after_ms or 1000)
    return cls(code_name, broker_code=code_name)


# --------------------------------------------------------------------------------------------- enums

TIMEFRAMES: dict[Timeframe, str] = {
    Timeframe.S1: "S1",
    Timeframe.S5: "S5",
    Timeframe.S15: "S15",
    Timeframe.M1: "M1",
    Timeframe.M3: "M3",
    Timeframe.M5: "M5",
    Timeframe.M10: "M10",
    Timeframe.M15: "M15",
    Timeframe.M30: "M30",
    Timeframe.H1: "H1",
    Timeframe.H4: "H4",
    Timeframe.D1: "D1",
    Timeframe.W1: "W1",
    Timeframe.MN1: "MN",
}

# Documented candle history depth per timeframe ("H1+ -> full available history").
HISTORY_DEPTH: dict[Timeframe, timedelta] = {
    Timeframe.S1: timedelta(days=1),
    Timeframe.S5: timedelta(days=5),
    Timeframe.S15: timedelta(days=15),
    Timeframe.M1: timedelta(days=365),
    Timeframe.M3: timedelta(days=365),
    Timeframe.M5: timedelta(days=730),
    Timeframe.M10: timedelta(days=730),
    Timeframe.M15: timedelta(days=1095),
    Timeframe.M30: timedelta(days=1095),
    Timeframe.H1: timedelta(days=3650),
    Timeframe.H4: timedelta(days=3650),
    Timeframe.D1: timedelta(days=3650),
    Timeframe.W1: timedelta(days=3650),
}


def side_to_wire(side: Side) -> str:
    return "buy" if side is Side.BUY else "sell"


def side_from_wire(value: str) -> Side:
    if value == "buy":
        return Side.BUY
    if value == "sell":
        return Side.SELL
    raise ValueError(f"unknown direction {value!r}")


# --------------------------------------------------------------------------------------------- symbols


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    instrument: Instrument
    exec_mode: str  # "instant" | "market" | "undefined" (deviation only applies to instant)
    order_types: frozenset[str]


def canonical_symbol(name: str, *, suffix: str, aliases: Mapping[str, str]) -> str:
    if name in aliases:
        return aliases[name]
    if suffix and name.endswith(suffix) and len(name) > len(suffix):
        name = name[: -len(suffix)]
    return name.upper()


_DAYS = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


def _week_seconds(value: str) -> int:
    day, _, clock = value.partition(",")
    if day.strip() not in _DAYS:
        raise ValueError(f"bad session time {value!r}")
    t = time.fromisoformat(clock.strip())
    return _DAYS[day.strip()] * 86_400 + t.hour * 3600 + t.minute * 60 + t.second


def sessions_from_wire(items: list[Json]) -> tuple[TradingSession, ...]:
    """``{"week_start_time": "Mon, 10:00:00", "week_end_time": "Mon, 11:59:59"}`` (UTC, inclusive end) ->
    per-day UTC windows (end exclusive). Sessions with ``trade_mode != enabled`` are skipped."""
    out: list[TradingSession] = []
    for item in items:
        if item.get("trade_mode", "enabled") != "enabled":
            continue
        start_raw, end_raw = item.get("week_start_time"), item.get("week_end_time")
        if not start_raw or not end_raw:
            continue
        start, end = _week_seconds(start_raw), _week_seconds(end_raw) + 1  # inclusive -> exclusive
        if end <= start:
            end += 7 * 86_400  # wraps over the week boundary
        cursor = start
        while cursor < end:
            day_end = (cursor // 86_400 + 1) * 86_400
            seg_end = min(end, day_end)
            sod = cursor % 86_400
            out.append(
                TradingSession(
                    weekday=(cursor // 86_400) % 7,
                    start=time(sod // 3600, sod % 3600 // 60, sod % 60),
                    end=time(0)
                    if seg_end == day_end
                    else time((seg_end % 86_400) // 3600, (seg_end % 3600) // 60, seg_end % 60),
                )
            )
            cursor = seg_end
    return tuple(out)


def instrument_from_wire(cond: Json, *, suffix: str, aliases: Mapping[str, str], hedging: bool) -> SymbolInfo:
    name = cond["instrument"]
    digits = int(cond["point_digits"])
    for key in ("contract_size", "volume_min", "volume_max", "volume_step"):
        if cond.get(key) in (None, ""):
            raise ValueError(f"{name}: missing {key}")
    inst = Instrument(
        symbol=canonical_symbol(name, suffix=suffix, aliases=aliases),
        broker_symbol=name,
        broker_id=name,
        digits=digits,
        tick_size=Decimal(1).scaleb(-digits),
        contract_size=dec(cond["contract_size"]),
        min_volume=dec(cond["volume_min"]),
        max_volume=dec(cond["volume_max"]),
        volume_step=dec(cond["volume_step"]),
        base_currency=cond.get("base_currency", ""),
        quote_currency=cond.get("quote_currency", ""),
        margin_currency=cond.get("margin_currency") or cond.get("base_currency", ""),
        hedging_allowed=hedging,
        sessions=sessions_from_wire(cond.get("trade_sessions") or []),
    )
    return SymbolInfo(inst, str(cond.get("exec_mode", "undefined")), frozenset(cond.get("order_types") or ()))


# --------------------------------------------------------------------------------------------- market data


def _px(value: object, digits: int) -> Decimal:
    return dec(value).quantize(Decimal(1).scaleb(-digits))


def tick_from_wire(msg: Json, symbol: str, digits: int, *, now: datetime, mono_ns: int) -> Tick:
    raw_ts = msg.get("timestamp")
    broker_ts: datetime | None = None
    if isinstance(raw_ts, str) and raw_ts:
        broker_ts = (
            parse_time(raw_ts) if not raw_ts.isdigit() else datetime.fromtimestamp(int(raw_ts) / 1000, UTC)
        )
    elif isinstance(raw_ts, int | float) and not isinstance(raw_ts, bool):
        broker_ts = datetime.fromtimestamp(raw_ts / 1000, UTC)
    return Tick(
        symbol=symbol,
        bid=_px(msg["bid"], digits),
        ask=_px(msg["ask"], digits),
        ts=broker_ts or now,
        recv_ts=now,
        recv_mono_ns=mono_ns,
        broker_ts=broker_ts,
    )


def bar_from_wire(c: Json, symbol: str, tf: Timeframe, digits: int, now: datetime) -> Bar:
    start = parse_time(c["time"])
    duration = tf.duration
    return Bar(
        symbol=symbol,
        timeframe=tf,
        start=start,
        open=_px(c["open"], digits),
        high=_px(c["high"], digits),
        low=_px(c["low"], digits),
        close=_px(c["close"], digits),
        volume=dec(c.get("volume", 0)),
        complete=duration is not None and start + duration <= now,
    )


# ------------------------------------------------------------------------------------------- trading entities

_ORDER_TYPES = {"market": OrderType.MARKET, "limit": OrderType.LIMIT, "stop": OrderType.STOP}
_STATES = {
    "pending_new": OrderStatus.ACCEPTED,
    "placed": OrderStatus.WORKING,
    "filled": OrderStatus.FILLED,
    "cancelled": OrderStatus.CANCELLED,
    "rejected": OrderStatus.REJECTED,
}


@dataclass(frozen=True, slots=True)
class Tag:
    """What only we know about an order: Exness returns neither the comment nor the client id on entities."""

    client_order_id: str
    strategy_id: str
    strategy_version: str


def order_from_wire(o: Json, info: SymbolInfo, tag: Tag | None, *, reason: str | None = None) -> Order:
    """Exness order (not ``sltp``) -> domain ``Order`` (broker view, single history entry)."""
    order_type = _ORDER_TYPES[o["order_type"]]
    digits = info.instrument.digits
    price = opt_dec(o.get("price"))
    status = _STATES.get(o.get("state", ""), OrderStatus.ACCEPTED)
    if order_type is OrderType.MARKET and status is OrderStatus.WORKING:
        status = OrderStatus.ACCEPTED
    request = OrderRequest(
        client_order_id=tag.client_order_id if tag else f"exness-{o['order_id']}",
        symbol=info.instrument.symbol,
        side=side_from_wire(o["direction"]),
        order_type=order_type,
        volume=dec(o["volume"]),
        strategy_id=tag.strategy_id if tag else MANUAL_STRATEGY,
        strategy_version=tag.strategy_version if tag else "",
        limit_price=_px(price, digits) if order_type is OrderType.LIMIT and price else None,
        stop_price=_px(price, digits) if order_type is OrderType.STOP and price else None,
        sl=opt_dec(o.get("stop_loss_price")),
        tp=opt_dec(o.get("take_profit_price")),
        time_in_force=TimeInForce.GTC,
    )
    ts = parse_time(o.get("update_time") or o["create_time"])
    filled = status is OrderStatus.FILLED
    return Order(
        request=request,
        status=status,
        history=(StatusChange(status=status, ts=ts, reason=reason),),
        broker_order_id=str(o["order_id"]),
        filled_volume=request.volume if filled else Decimal(0),
        avg_fill_price=_px(price, digits) if filled and price else None,
        rejection_reason=reason if status is OrderStatus.REJECTED else None,
    )


def position_from_wire(
    p: Json, info: SymbolInfo, *, strategy_id: str, sl: Decimal | None, tp: Decimal | None
) -> Position:
    return Position(
        position_id=str(p["position_id"]),
        symbol=info.instrument.symbol,
        side=side_from_wire(p["direction"]),
        volume=dec(p["volume"]),
        entry_price=_px(p["open_price"], info.instrument.digits),
        opened_ts=parse_time(p["create_time"]),
        strategy_id=strategy_id,
        sl=sl,
        tp=tp,
        swap=opt_dec(p.get("swap")) or Decimal(0),
        commission=opt_dec(p.get("commission")) or Decimal(0),
    )
