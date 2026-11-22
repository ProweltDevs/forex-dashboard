"""Pure conversions between cTrader Open API wire data and domain objects.

Facts used here are from the vendored protos (src/trader/brokers/ctrader/proto/src) and
https://help.ctrader.com/open-api/ ; see docs/brokers/ctrader.md for citations.

- Volumes are integers in 0.01 of a unit ("cents"). ``lotSize`` is also in cents, so
  ``wire_volume = lots * lotSize`` and ``contract_size (units) = lotSize / 100``.
- Spot, trendbar and relative SL/TP prices are integers in 1/100000 of a price unit.
- Absolute prices in orders/positions are ``double``; we round to the symbol's digits both ways.
- Money is an integer scaled by ``10**moneyDigits``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    UnknownSymbol,
)
from trader.domain.models import Bar, Instrument, Order, OrderRequest, Position, StatusChange, TradingSession
from trader.domain.time import from_epoch_ms

from .proto import OpenApiModelMessages_pb2 as model

PRICE_SCALE = Decimal(100_000)
MANUAL_STRATEGY = "manual"
_LABEL_SEP = "|"

# --------------------------------------------------------------------------------------------- units


def quantum(digits: int) -> Decimal:
    return Decimal(1).scaleb(-digits)


def price_from_int(value: int, digits: int) -> Decimal:
    """1/100000-scaled integer (spots, trendbars) -> Decimal rounded to symbol digits."""
    return (Decimal(value) / PRICE_SCALE).quantize(quantum(digits))


def price_from_double(value: float, digits: int) -> Decimal:
    """Absolute ``double`` price from the wire -> Decimal rounded to symbol digits."""
    return Decimal(repr(value)).quantize(quantum(digits))


def price_to_double(value: Decimal, digits: int) -> float:
    """Decimal -> wire ``double``. cTrader rejects prices with more precision than the symbol digits."""
    return float(value.quantize(quantum(digits)))


def relative_to_int(distance: Decimal) -> int:
    """Price distance -> 1/100000-scaled integer (relative SL/TP)."""
    scaled = distance * PRICE_SCALE
    if scaled != scaled.to_integral_value():
        raise InvalidPrice(f"distance {distance} is finer than 1/100000")
    return int(scaled)


def lots_to_wire(lots: Decimal, lot_size: int) -> int:
    """Domain lots -> cTrader volume in cents of units."""
    wire = lots * lot_size
    if wire != wire.to_integral_value():
        raise InvalidVolume(f"{lots} lots is not representable with lotSize {lot_size}")
    return int(wire)


def lots_from_wire(volume: int, lot_size: int) -> Decimal:
    return Decimal(volume) / Decimal(lot_size)


def money(value: int, money_digits: int) -> Decimal:
    return Decimal(value).scaleb(-money_digits)


# --------------------------------------------------------------------------------------------- enums

TIMEFRAMES: dict[Timeframe, model.ProtoOATrendbarPeriod.ValueType] = {
    Timeframe.M1: model.M1,
    Timeframe.M3: model.M3,
    Timeframe.M5: model.M5,
    Timeframe.M10: model.M10,
    Timeframe.M15: model.M15,
    Timeframe.M30: model.M30,
    Timeframe.H1: model.H1,
    Timeframe.H4: model.H4,
    Timeframe.D1: model.D1,
    Timeframe.W1: model.W1,
    Timeframe.MN1: model.MN1,
}

_SIDE_TO_WIRE: dict[Side, model.ProtoOATradeSide.ValueType] = {Side.BUY: model.BUY, Side.SELL: model.SELL}
_SIDE_FROM_WIRE = {v: k for k, v in _SIDE_TO_WIRE.items()}

_TYPE_TO_WIRE: dict[OrderType, model.ProtoOAOrderType.ValueType] = {
    OrderType.MARKET: model.MARKET,
    OrderType.LIMIT: model.LIMIT,
    OrderType.STOP: model.STOP,
    OrderType.STOP_LIMIT: model.STOP_LIMIT,
}
_TYPE_FROM_WIRE = {**{v: k for k, v in _TYPE_TO_WIRE.items()}, model.MARKET_RANGE: OrderType.MARKET}

_TIF_TO_WIRE: dict[TimeInForce, model.ProtoOATimeInForce.ValueType] = {
    TimeInForce.GTC: model.GOOD_TILL_CANCEL,
    TimeInForce.GTD: model.GOOD_TILL_DATE,
    TimeInForce.IOC: model.IMMEDIATE_OR_CANCEL,
    TimeInForce.FOK: model.FILL_OR_KILL,
}
_TIF_FROM_WIRE = {**{v: k for k, v in _TIF_TO_WIRE.items()}, model.MARKET_ON_OPEN: TimeInForce.GTC}

PENDING_WIRE_TYPES = frozenset({model.LIMIT, model.STOP, model.STOP_LIMIT})


def side_to_wire(side: Side) -> model.ProtoOATradeSide.ValueType:
    return _SIDE_TO_WIRE[side]


def side_from_wire(value: model.ProtoOATradeSide.ValueType) -> Side:
    return _SIDE_FROM_WIRE[value]


def order_type_to_wire(order_type: OrderType) -> model.ProtoOAOrderType.ValueType:
    return _TYPE_TO_WIRE[order_type]


def tif_to_wire(tif: TimeInForce) -> model.ProtoOATimeInForce.ValueType:
    if tif is TimeInForce.DAY:
        raise OrderRejected("cTrader has no DAY time in force; use GTD with an expiry")
    return _TIF_TO_WIRE[tif]


# --------------------------------------------------------------------------------------------- errors

_ERRORS: dict[str, type[BrokerError]] = {
    "MARKET_CLOSED": MarketClosed,
    "SYMBOL_HAS_HOLIDAY": MarketClosed,
    "TRADING_DISABLED": MarketClosed,
    "NO_QUOTES": MarketClosed,
    "NOT_ENOUGH_MONEY": InsufficientMargin,
    "TRADING_BAD_VOLUME": InvalidVolume,
    "TRADING_BAD_STAKE": InvalidVolume,
    "TRADING_BAD_STOPS": InvalidPrice,
    "TRADING_BAD_PRICES": InvalidPrice,
    "PROTECTION_IS_TOO_CLOSE_TO_MARKET": InvalidPrice,
    "TRADING_BAD_EXPIRATION_DATE": InvalidPrice,
    "REQUEST_FREQUENCY_EXCEEDED": RateLimited,
    "BLOCKED_PAYLOAD_TYPE": RateLimited,
    "OA_AUTH_TOKEN_EXPIRED": AuthExpired,
    "CH_ACCESS_TOKEN_INVALID": AuthExpired,
    "ACCOUNT_NOT_AUTHORIZED": AuthExpired,
    "CH_CLIENT_NOT_AUTHENTICATED": AuthExpired,
    "CH_SERVER_NOT_REACHABLE": BrokerUnavailable,
    "SERVER_IS_UNDER_MAINTENANCE": BrokerUnavailable,
    "CANT_ROUTE_REQUEST": BrokerUnavailable,
    "CONNECTIONS_LIMIT_EXCEEDED": BrokerUnavailable,
    "ORDER_NOT_FOUND": OrderNotFound,
    "POSITION_NOT_FOUND": PositionNotFound,
    "POSITION_NOT_OPEN": PositionNotFound,
    "SYMBOL_NOT_FOUND": UnknownSymbol,
    "UNKNOWN_SYMBOL": UnknownSymbol,
    "TIMEOUT_ERROR": OrderRejected,  # "Deal execution is reached timeout and rejected"
    "MAX_EXPOSURE_REACHED": OrderRejected,
    "TOO_MANY_POSITIONS": OrderRejected,
    "SHORT_SELLING_NOT_ALLOWED": OrderRejected,
    "TRADING_NOT_ALLOWED": OrderRejected,
    "UNABLE_TO_CANCEL_ORDER": OrderRejected,
    "UNABLE_TO_AMEND_ORDER": OrderRejected,
    "PENDING_EXECUTION": OrderRejected,
    "POSITION_LOCKED": OrderRejected,
    "CONCURRENT_MODIFICATION": OrderRejected,
}


def map_error(code: str, description: str = "", *, retry_after_s: int | None = None) -> BrokerError:
    """cTrader error code -> canonical error. The raw code is kept in ``broker_code`` for logs only."""
    cls = _ERRORS.get(code, BrokerError)
    message = f"{code}: {description}" if description else code
    if cls is RateLimited:
        retry_ms = retry_after_s * 1000 if retry_after_s else 1000
        return RateLimited(message, broker_code=code, retry_after_ms=retry_ms)
    return cls(message, broker_code=code)


# --------------------------------------------------------------------------------------------- labels


def make_label(strategy_id: str, strategy_version: str) -> str:
    """Order label (max 100 chars) carrying the strategy tag, so orphan orders can be attributed."""
    return f"{strategy_id}{_LABEL_SEP}{strategy_version}"[:100]


def parse_label(label: str) -> tuple[str, str]:
    if not label:
        return MANUAL_STRATEGY, ""
    strategy, _, version = label.partition(_LABEL_SEP)
    return strategy or MANUAL_STRATEGY, version


# --------------------------------------------------------------------------------------------- symbols


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    """What the adapter needs per symbol: the instrument plus wire details that stay in the adapter."""

    instrument: Instrument
    symbol_id: int
    lot_size: int  # cents of units per lot


def canonical_symbol(broker_name: str, aliases: Mapping[str, str]) -> str:
    """Broker symbol name -> canonical. ``aliases`` maps broker name -> canonical for odd names."""
    if broker_name in aliases:
        return aliases[broker_name]
    return broker_name.replace("/", "").replace(" ", "").upper()


def sessions_from_schedule(
    intervals: list[tuple[int, int]], tz_name: str, *, reference: datetime
) -> tuple[TradingSession, ...]:
    """cTrader schedule (seconds from Sunday 00:00 in ``tz_name``) -> weekly UTC windows split at midnight.

    DST shifts the UTC offset, so this is computed for the week containing ``reference`` and must be
    refreshed when instruments are reloaded."""
    try:
        tz = ZoneInfo(tz_name or "UTC")
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")
    local_ref = reference.astimezone(tz)
    # Sunday 00:00 local of the reference week (Python weekday: Mon=0 .. Sun=6)
    sunday = (local_ref - timedelta(days=(local_ref.weekday() + 1) % 7)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sessions: list[TradingSession] = []
    for start_s, end_s in intervals:
        start = (sunday + timedelta(seconds=start_s)).astimezone(UTC)
        end = (sunday + timedelta(seconds=end_s)).astimezone(UTC)
        cursor = start
        while cursor < end:
            next_midnight = (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            seg_end = min(end, next_midnight)
            sessions.append(
                TradingSession(
                    weekday=cursor.weekday(),
                    start=cursor.time().replace(tzinfo=None),
                    end=time(0) if seg_end == next_midnight else seg_end.time().replace(tzinfo=None),
                )
            )
            cursor = seg_end
    return tuple(sessions)


def instrument_from_wire(
    light: model.ProtoOALightSymbol,
    full: model.ProtoOASymbol,
    assets: Mapping[int, str],
    *,
    aliases: Mapping[str, str],
    hedging: bool,
    reference: datetime,
) -> SymbolInfo:
    digits = full.digits
    lot_size = full.lotSize
    if lot_size <= 0:
        raise ValueError(f"symbol {light.symbolName}: missing lotSize")
    base = assets.get(light.baseAssetId, "")
    quote = assets.get(light.quoteAssetId, "")
    inst = Instrument(
        symbol=canonical_symbol(light.symbolName, aliases),
        broker_symbol=light.symbolName,
        broker_id=str(full.symbolId),
        digits=digits,
        tick_size=quantum(digits),
        contract_size=Decimal(lot_size) / 100,
        min_volume=lots_from_wire(full.minVolume, lot_size),
        max_volume=lots_from_wire(full.maxVolume, lot_size),
        volume_step=lots_from_wire(full.stepVolume, lot_size),
        base_currency=base,
        quote_currency=quote,
        margin_currency=base,  # cTrader margin is computed on the base asset (marginRate = Base/Deposit)
        hedging_allowed=hedging,
        sessions=sessions_from_schedule(
            [(i.startSecond, i.endSecond) for i in full.schedule], full.scheduleTimeZone, reference=reference
        ),
    )
    return SymbolInfo(instrument=inst, symbol_id=full.symbolId, lot_size=lot_size)


# --------------------------------------------------------------------------------------------- bars


def bar_from_wire(tb: model.ProtoOATrendbar, symbol: str, tf: Timeframe, digits: int, now: datetime) -> Bar:
    start = from_epoch_ms(tb.utcTimestampInMinutes * 60_000)
    duration = tf.duration
    complete = duration is not None and start + duration <= now
    return Bar(
        symbol=symbol,
        timeframe=tf,
        start=start,
        open=price_from_int(tb.low + tb.deltaOpen, digits),
        high=price_from_int(tb.low + tb.deltaHigh, digits),
        low=price_from_int(tb.low, digits),
        close=price_from_int(tb.low + tb.deltaClose, digits),
        volume=Decimal(tb.volume),  # ticks
        complete=complete,
    )


# --------------------------------------------------------------------------------------------- orders


def _status(order: model.ProtoOAOrder) -> OrderStatus:
    status = order.orderStatus
    executed = order.executedVolume
    if status == model.ORDER_STATUS_FILLED:
        return OrderStatus.FILLED
    if status == model.ORDER_STATUS_REJECTED:
        return OrderStatus.REJECTED
    if status == model.ORDER_STATUS_EXPIRED:
        return OrderStatus.EXPIRED
    if status == model.ORDER_STATUS_CANCELLED:
        return OrderStatus.CANCELLED
    if 0 < executed < order.tradeData.volume:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.WORKING if order.orderType in PENDING_WIRE_TYPES else OrderStatus.ACCEPTED


def is_internal_order(order: model.ProtoOAOrder) -> bool:
    """Closing orders and SL/TP protection orders are broker-internal: we report their fills and
    position changes, not the orders themselves."""
    return order.closingOrder or order.orderType == model.STOP_LOSS_TAKE_PROFIT


_PriceField = Literal["limitPrice", "stopPrice", "stopLoss", "takeProfit", "executionPrice"]


def order_from_wire(order: model.ProtoOAOrder, info: SymbolInfo, *, reason: str | None = None) -> Order:
    """Broker view of an order as a domain ``Order`` (history = one entry at the last update)."""
    inst = info.instrument
    digits = inst.digits
    td = order.tradeData
    strategy, version = parse_label(td.label)
    order_type = _TYPE_FROM_WIRE.get(order.orderType, OrderType.MARKET)
    tif = _TIF_FROM_WIRE.get(order.timeInForce, TimeInForce.GTC)
    expiry = from_epoch_ms(order.expirationTimestamp) if order.HasField("expirationTimestamp") else None
    if tif is TimeInForce.GTD and expiry is None:
        tif = TimeInForce.GTC
    if tif is not TimeInForce.GTD:
        expiry = None
    if order_type is OrderType.MARKET and tif not in (TimeInForce.IOC, TimeInForce.FOK, TimeInForce.GTC):
        tif = TimeInForce.IOC

    def opt(field: _PriceField) -> Decimal | None:
        # A zero/negative price means "not set" (proto2 optional doubles can be present with 0).
        if not order.HasField(field) or getattr(order, field) <= 0:
            return None
        return price_from_double(getattr(order, field), digits)

    limit_price = opt("limitPrice") if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) else None
    stop_price = opt("stopPrice") if order_type in (OrderType.STOP, OrderType.STOP_LIMIT) else None
    if order_type is OrderType.STOP_LIMIT and limit_price is None and stop_price is not None:
        # cTrader expresses STOP_LIMIT as stopPrice + slippageInPoints; derive the limit.
        slip = Decimal(order.slippageInPoints) * inst.tick_size
        side = side_from_wire(td.tradeSide)
        limit_price = stop_price + slip if side is Side.BUY else stop_price - slip

    request = OrderRequest(
        client_order_id=order.clientOrderId or f"ctrader-{order.orderId}",
        symbol=inst.symbol,
        side=side_from_wire(td.tradeSide),
        order_type=order_type,
        volume=lots_from_wire(td.volume, info.lot_size),
        strategy_id=strategy,
        strategy_version=version,
        limit_price=limit_price,
        stop_price=stop_price,
        sl=opt("stopLoss"),
        tp=opt("takeProfit"),
        time_in_force=tif,
        expiry=expiry,
    )
    status = _status(order)
    ts_ms = order.utcLastUpdateTimestamp or td.openTimestamp
    filled = lots_from_wire(order.executedVolume, info.lot_size)
    return Order(
        request=request,
        status=status,
        history=(StatusChange(status=status, ts=from_epoch_ms(ts_ms), reason=reason),),
        broker_order_id=str(order.orderId),
        filled_volume=min(filled, request.volume),
        avg_fill_price=opt("executionPrice") if filled > 0 else None,
        rejection_reason=reason if status is OrderStatus.REJECTED else None,
    )


def position_from_wire(
    pos: model.ProtoOAPosition, info: SymbolInfo, *, unrealized: Decimal = Decimal(0)
) -> Position:
    inst = info.instrument
    td = pos.tradeData
    strategy, _ = parse_label(td.label)
    digits_money = pos.moneyDigits
    return Position(
        position_id=str(pos.positionId),
        symbol=inst.symbol,
        side=side_from_wire(td.tradeSide),
        volume=lots_from_wire(td.volume, info.lot_size),
        entry_price=price_from_double(pos.price, inst.digits),
        opened_ts=from_epoch_ms(td.openTimestamp),
        strategy_id=strategy,
        sl=price_from_double(pos.stopLoss, inst.digits) if pos.HasField("stopLoss") else None,
        tp=price_from_double(pos.takeProfit, inst.digits) if pos.HasField("takeProfit") else None,
        unrealized_pnl=unrealized,
        swap=money(pos.swap, digits_money),
        commission=money(pos.commission, digits_money),
    )
