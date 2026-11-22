"""Pure cTrader conversions: units, prices, trendbars, sessions, labels, errors, order/position mapping."""

from __future__ import annotations

from datetime import UTC, datetime, time
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trader.brokers.ctrader import mapping as mp
from trader.brokers.ctrader.proto import OpenApiModelMessages_pb2 as model
from trader.domain import OrderStatus, OrderType, Side, Timeframe, TimeInForce
from trader.domain.errors import (
    AuthExpired,
    BrokerError,
    InsufficientMargin,
    InvalidPrice,
    InvalidVolume,
    MarketClosed,
    OrderRejected,
    RateLimited,
)
from trader.domain.time import from_epoch_ms

D = Decimal
LOT = 10_000_000  # EURUSD lotSize in cents (100 000 units)
NOW = datetime(2026, 1, 7, 12, 0, tzinfo=UTC)


def eurusd_info() -> mp.SymbolInfo:
    light = model.ProtoOALightSymbol(
        symbolId=1, symbolName="EUR/USD", enabled=True, baseAssetId=1, quoteAssetId=2
    )
    full = model.ProtoOASymbol(
        symbolId=1,
        digits=5,
        pipPosition=4,
        lotSize=LOT,
        minVolume=100_000,
        maxVolume=5_000_000_000,
        stepVolume=100_000,
        scheduleTimeZone="UTC",
    )
    return mp.instrument_from_wire(light, full, {1: "EUR", 2: "USD"}, aliases={}, hedging=True, reference=NOW)


# ------------------------------------------------------------------ units


@given(st.integers(min_value=1, max_value=500_000))
def test_volume_round_trip(hundredths: int) -> None:
    lots = D(hundredths) / 100
    wire = mp.lots_to_wire(lots, LOT)
    assert wire == hundredths * 100_000  # 0.01 lot = 1000 units = 100 000 cents
    assert mp.lots_from_wire(wire, LOT) == lots


def test_volume_not_representable() -> None:
    with pytest.raises(InvalidVolume):
        mp.lots_to_wire(D("0.00000001"), LOT)  # = 0.1 cent


@given(st.integers(min_value=1, max_value=10**9), st.sampled_from([2, 3, 5]))
def test_int_prices_scale_by_100000(value: int, digits: int) -> None:
    price = mp.price_from_int(value, digits)
    assert price == (D(value) / 100_000).quantize(mp.quantum(digits))


@given(st.decimals(min_value=D("0.5"), max_value=D(5000), places=5))
def test_double_prices_round_trip_at_digits(price: Decimal) -> None:
    assert mp.price_from_double(mp.price_to_double(price, 5), 5) == price


def test_relative_and_money() -> None:
    assert mp.relative_to_int(D("0.0012")) == 120
    with pytest.raises(InvalidPrice):
        mp.relative_to_int(D("0.000001"))
    assert mp.money(10053099944, 8) == D("100.53099944")
    assert mp.money(-30, 2) == D("-0.30")


# ------------------------------------------------------------------ symbols


def test_instrument_from_wire() -> None:
    inst = eurusd_info().instrument
    assert inst.symbol == "EURUSD"  # "EUR/USD" canonicalised
    assert inst.broker_symbol == "EUR/USD"
    assert inst.contract_size == D(100_000)
    assert (inst.min_volume, inst.volume_step, inst.max_volume) == (D("0.01"), D("0.01"), D(500))
    assert inst.tick_size == D("0.00001")
    assert (inst.base_currency, inst.quote_currency, inst.margin_currency) == ("EUR", "USD", "EUR")


def test_aliases() -> None:
    assert mp.canonical_symbol("GOLD", {"GOLD": "XAUUSD"}) == "XAUUSD"
    assert mp.canonical_symbol("eur/usd", {}) == "EURUSD"


def test_sessions_from_schedule_split_at_midnight() -> None:
    # Sunday 22:00 -> Friday 21:00 (seconds from Sunday 00:00 UTC)
    sessions = mp.sessions_from_schedule([(22 * 3600, 5 * 86400 + 21 * 3600)], "UTC", reference=NOW)
    assert sessions[0].weekday == 6  # Sunday
    assert (sessions[0].start, sessions[0].end) == (time(22), time(0))
    assert sessions[-1].weekday == 4  # Friday
    assert sessions[-1].end == time(21)
    assert len(sessions) == 6
    inst = eurusd_info().instrument
    from dataclasses import replace

    with_sessions = replace(inst, sessions=sessions)
    assert with_sessions.is_open(datetime(2026, 1, 7, 3, tzinfo=UTC))  # Wednesday
    assert not with_sessions.is_open(datetime(2026, 1, 10, 12, tzinfo=UTC))  # Saturday
    assert not with_sessions.is_open(datetime(2026, 1, 9, 21, 30, tzinfo=UTC))  # Friday after close


def test_sessions_respect_timezone() -> None:
    # 00:00-01:00 Sunday in New York (EST, UTC-5 in January) = 05:00-06:00 UTC Sunday
    sessions = mp.sessions_from_schedule([(0, 3600)], "America/New_York", reference=NOW)
    assert [(s.weekday, s.start, s.end) for s in sessions] == [(6, time(5), time(6))]


# ------------------------------------------------------------------ bars


def test_trendbar_decoding() -> None:
    tb = model.ProtoOATrendbar(
        volume=42,
        period=model.M5,
        low=109_990,
        deltaOpen=5,
        deltaClose=8,
        deltaHigh=20,
        utcTimestampInMinutes=29_000_000,
    )
    bar = mp.bar_from_wire(tb, "EURUSD", Timeframe.M5, 5, NOW)
    assert (bar.open, bar.high, bar.low, bar.close) == (
        D("1.09995"),
        D("1.10010"),
        D("1.09990"),
        D("1.09998"),
    )
    assert bar.volume == 42
    assert bar.start == from_epoch_ms(29_000_000 * 60_000)
    assert bar.complete
    assert set(mp.TIMEFRAMES) >= {Timeframe.M1, Timeframe.H1, Timeframe.D1}
    assert Timeframe.S1 not in mp.TIMEFRAMES


# ------------------------------------------------------------------ errors, labels


@pytest.mark.parametrize(
    ("code", "cls"),
    [
        ("MARKET_CLOSED", MarketClosed),
        ("NOT_ENOUGH_MONEY", InsufficientMargin),
        ("TRADING_BAD_VOLUME", InvalidVolume),
        ("TRADING_BAD_STOPS", InvalidPrice),
        ("CH_ACCESS_TOKEN_INVALID", AuthExpired),
        ("MAX_EXPOSURE_REACHED", OrderRejected),
        ("SOMETHING_NEW", BrokerError),
    ],
)
def test_error_mapping(code: str, cls: type[BrokerError]) -> None:
    err = mp.map_error(code, "desc")
    assert type(err) is cls
    assert err.broker_code == code


def test_rate_limit_error_carries_retry() -> None:
    err = mp.map_error("BLOCKED_PAYLOAD_TYPE", retry_after_s=3)
    assert isinstance(err, RateLimited)
    assert err.retry_after_ms == 3000


def test_labels() -> None:
    assert mp.parse_label(mp.make_label("ema_cross", "2")) == ("ema_cross", "2")
    assert mp.parse_label("") == ("manual", "")
    assert mp.parse_label("set-by-hand") == ("set-by-hand", "")
    with pytest.raises(OrderRejected):
        mp.tif_to_wire(TimeInForce.DAY)


# ------------------------------------------------------------------ orders, positions


def _order(**kw: object) -> model.ProtoOAOrder:
    fields: dict[str, object] = {
        "orderId": 7,
        "orderType": model.LIMIT,
        "orderStatus": model.ORDER_STATUS_ACCEPTED,
        "limitPrice": 1.095,
        "clientOrderId": "01J00000000000000000000000",
        "timeInForce": model.GOOD_TILL_CANCEL,
        "utcLastUpdateTimestamp": 1_767_000_000_000,
        "tradeData": model.ProtoOATradeData(
            symbolId=1, volume=1_000_000, tradeSide=model.BUY, openTimestamp=1_767_000_000_000, label="s1|3"
        ),
    }
    fields.update(kw)
    return model.ProtoOAOrder(**fields)  # type: ignore[arg-type]


def test_pending_order_from_wire() -> None:
    order = mp.order_from_wire(_order(stopLoss=1.09), eurusd_info())
    assert order.status is OrderStatus.WORKING
    assert order.request.order_type is OrderType.LIMIT
    assert order.request.limit_price == D("1.09500")
    assert order.request.sl == D("1.09000")
    assert order.request.volume == D("0.1")
    assert (order.request.strategy_id, order.request.strategy_version) == ("s1", "3")
    assert order.broker_order_id == "7"


def test_order_statuses_and_internal_orders() -> None:
    info = eurusd_info()
    partial = mp.order_from_wire(_order(executedVolume=500_000, executionPrice=1.095), info)
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    assert partial.filled_volume == D("0.05")
    rejected = mp.order_from_wire(
        _order(orderStatus=model.ORDER_STATUS_REJECTED), info, reason="NOT_ENOUGH_MONEY"
    )
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.rejection_reason == "NOT_ENOUGH_MONEY"
    gtd = mp.order_from_wire(
        _order(timeInForce=model.GOOD_TILL_DATE, expirationTimestamp=1_767_100_000_000), info
    )
    assert gtd.request.time_in_force is TimeInForce.GTD
    assert gtd.request.expiry is not None
    no_cid = mp.order_from_wire(_order(clientOrderId=""), info)
    assert no_cid.request.client_order_id == "ctrader-7"
    assert mp.is_internal_order(_order(closingOrder=True))
    assert mp.is_internal_order(_order(orderType=model.STOP_LOSS_TAKE_PROFIT))


def test_stop_limit_derives_limit_from_slippage_points() -> None:
    order = mp.order_from_wire(
        _order(orderType=model.STOP_LIMIT, stopPrice=1.101, slippageInPoints=20, limitPrice=0), eurusd_info()
    )
    assert order.request.stop_price == D("1.10100")
    assert order.request.limit_price is not None


def test_position_from_wire() -> None:
    wire = model.ProtoOAPosition(
        positionId=55,
        positionStatus=model.POSITION_STATUS_OPEN,
        swap=-120,
        price=1.1001,
        stopLoss=1.09,
        commission=-30,
        moneyDigits=2,
        tradeData=model.ProtoOATradeData(
            symbolId=1, volume=2_000_000, tradeSide=model.SELL, openTimestamp=1_767_000_000_000
        ),
    )
    pos = mp.position_from_wire(wire, eurusd_info(), unrealized=D("12.5"))
    assert pos.position_id == "55"
    assert pos.side is Side.SELL
    assert pos.volume == D("0.2")
    assert pos.entry_price == D("1.10010")
    assert pos.swap == D("-1.20")
    assert pos.commission == D("-0.30")
    assert pos.strategy_id == "manual"
    assert pos.tp is None
