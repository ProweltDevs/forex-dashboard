from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.factories import T0, eurusd, intent, request
from trader.domain import (
    AccountState,
    Bar,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    Position,
    Side,
    Tick,
    Timeframe,
    TimeInForce,
    TradingSession,
)
from trader.domain.errors import InvalidPrice, InvalidVolume
from trader.domain.money import is_multiple_of

# ------------------------------------------------------------------ enums


def test_side_helpers() -> None:
    assert Side.BUY.opposite is Side.SELL
    assert Side.SELL.sign == -1


def test_terminal_statuses() -> None:
    terminal = {s for s in OrderStatus if s.is_terminal}
    assert terminal == {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}


def test_timeframe_durations() -> None:
    assert Timeframe.M5.duration == timedelta(minutes=5)
    assert Timeframe.MN1.duration is None
    assert all(tf.duration is not None for tf in Timeframe if tf is not Timeframe.MN1)


# ------------------------------------------------------------------ instrument


@given(st.decimals(min_value=Decimal("0.01"), max_value=1000, places=4))
def test_normalize_volume_rounds_down_within_limits(lots: Decimal) -> None:
    inst = eurusd()
    result = inst.normalize_volume(lots)
    assert result <= lots
    assert inst.min_volume <= result <= inst.max_volume
    assert is_multiple_of(result, inst.volume_step)
    assert inst.is_valid_volume(result)


def test_normalize_volume_below_min_raises() -> None:
    with pytest.raises(InvalidVolume):
        eurusd().normalize_volume(Decimal("0.009"))


def test_normalize_volume_never_rounds_up() -> None:
    assert eurusd().normalize_volume(Decimal("0.0199")) == Decimal("0.01")


def test_round_and_check_price() -> None:
    inst = eurusd()
    assert inst.round_price(Decimal("1.234567")) == Decimal("1.23457")
    inst.check_price(Decimal("1.23457"))
    with pytest.raises(InvalidPrice):
        inst.check_price(Decimal("1.234567"))


def test_gold_tick_grid() -> None:
    xau = eurusd(symbol="XAUUSD", digits=2, tick_size=Decimal("0.01"), contract_size=Decimal(100))
    assert xau.round_price(Decimal("2400.123")) == Decimal("2400.12")
    assert xau.lots_to_units(Decimal("0.5")) == Decimal(50)


@given(st.decimals(min_value=Decimal("0.01"), max_value=100, places=2))
def test_lots_units_round_trip(lots: Decimal) -> None:
    inst = eurusd()
    assert inst.units_to_lots(inst.lots_to_units(lots)) == lots


def test_instrument_validation() -> None:
    with pytest.raises(ValueError, match="min_volume"):
        eurusd(min_volume=Decimal(10), max_volume=Decimal(1))
    with pytest.raises(TypeError):
        eurusd(tick_size=0.00001)
    with pytest.raises(ValueError, match="weekday"):
        TradingSession(weekday=7, start=time(0), end=time(23))


# ------------------------------------------------------------------ tick / bar


def test_tick_prices() -> None:
    tick = Tick(
        symbol="EURUSD", bid=Decimal("1.10000"), ask=Decimal("1.10010"), ts=T0, recv_ts=T0, recv_mono_ns=1
    )
    assert tick.spread == Decimal("0.00010")
    assert tick.mid == Decimal("1.10005")
    assert tick.price_for(Side.BUY) == tick.ask
    assert tick.price_for(Side.SELL) == tick.bid


def test_bar_validates_ohlc_and_time() -> None:
    d = Decimal
    ok = Bar(
        symbol="EURUSD",
        timeframe=Timeframe.M1,
        start=T0,
        open=d(1),
        high=d(2),
        low=d("0.5"),
        close=d("1.5"),
        volume=d(10),
        complete=True,
    )
    assert ok.complete
    with pytest.raises(ValueError, match="OHLC"):
        Bar(
            symbol="EURUSD",
            timeframe=Timeframe.M1,
            start=T0,
            open=d(1),
            high=d("0.9"),
            low=d("0.5"),
            close=d("0.8"),
            volume=d(1),
            complete=True,
        )
    with pytest.raises(ValueError, match="naive"):
        Bar(
            symbol="EURUSD",
            timeframe=Timeframe.M1,
            start=datetime(2026, 1, 1),  # noqa: DTZ001
            open=d(1),
            high=d(1),
            low=d(1),
            close=d(1),
            volume=d(0),
            complete=False,
        )


# ------------------------------------------------------------------ orders


def test_intent_to_request_copies_fields() -> None:
    i = intent(order_type=OrderType.LIMIT, limit_price=Decimal("1.1"), sl=Decimal("1.09"), tags=("a",))
    r = i.to_request("01J00000000000000000000000", volume=Decimal("0.05"))
    assert r.volume == Decimal("0.05")
    assert (r.symbol, r.side, r.limit_price, r.sl, r.tags) == (i.symbol, i.side, i.limit_price, i.sl, i.tags)
    assert r.signal_ts == i.signal_ts
    assert r.entry_price == Decimal("1.1")


@pytest.mark.parametrize(
    ("order_type", "limit_price", "stop_price"),
    [
        (OrderType.MARKET, Decimal(1), None),
        (OrderType.MARKET, None, Decimal(1)),
        (OrderType.LIMIT, None, None),
        (OrderType.LIMIT, Decimal(1), Decimal(1)),
        (OrderType.STOP, None, None),
        (OrderType.STOP, Decimal(1), None),
        (OrderType.STOP_LIMIT, Decimal(1), None),
        (OrderType.STOP_LIMIT, None, Decimal(1)),
    ],
)
def test_price_fields_must_match_order_type(
    order_type: OrderType, limit_price: Decimal | None, stop_price: Decimal | None
) -> None:
    with pytest.raises(ValueError, match="price"):
        intent(order_type=order_type, limit_price=limit_price, stop_price=stop_price)


def test_valid_order_types() -> None:
    intent(order_type=OrderType.STOP, stop_price=Decimal("1.2"))
    stop_limit = intent(
        order_type=OrderType.STOP_LIMIT, stop_price=Decimal("1.2"), limit_price=Decimal("1.21")
    )
    assert stop_limit.order_type is OrderType.STOP_LIMIT


def test_order_field_validation() -> None:
    with pytest.raises(ValueError, match="volume"):
        intent(volume=Decimal(0))
    with pytest.raises(TypeError):
        intent(volume=0.1)
    with pytest.raises(ValueError, match="expiry"):
        intent(order_type=OrderType.LIMIT, limit_price=Decimal(1), time_in_force=TimeInForce.GTD)
    with pytest.raises(ValueError, match="expiry"):
        intent(order_type=OrderType.LIMIT, limit_price=Decimal(1), expiry=T0)
    with pytest.raises(ValueError, match="MARKET"):
        intent(time_in_force=TimeInForce.DAY)
    with pytest.raises(ValueError, match="naive"):
        intent(signal_ts=datetime(2026, 1, 1))  # noqa: DTZ001
    with pytest.raises(ValueError, match="client_order_id"):
        intent().to_request("abc")
    gtd = intent(
        order_type=OrderType.LIMIT,
        limit_price=Decimal(1),
        time_in_force=TimeInForce.GTD,
        expiry=T0 + timedelta(hours=1),
    )
    assert gtd.expiry is not None


def test_order_invariants() -> None:
    order = Order.new(request(), T0)
    assert order.status is OrderStatus.NEW
    assert order.remaining_volume == Decimal("1.00")
    with pytest.raises(ValueError, match="history"):
        Order(request=request(), status=OrderStatus.WORKING, history=order.history)
    with pytest.raises(ValueError, match="exceeds"):
        Order(request=request(), status=OrderStatus.NEW, history=order.history, filled_volume=Decimal(2))


# ------------------------------------------------------------------ fills / positions / account


def test_fill_position_account_validation() -> None:
    d = Decimal
    Fill(
        fill_id="1",
        client_order_id=None,
        broker_order_id="b",
        position_id="p",
        symbol="EURUSD",
        side=Side.BUY,
        volume=d("0.1"),
        price=d("1.1"),
        ts=T0,
        commission=d("-0.7"),
    )
    with pytest.raises(ValueError, match="price"):
        Fill(
            fill_id="1",
            client_order_id=None,
            broker_order_id="b",
            position_id="p",
            symbol="EURUSD",
            side=Side.BUY,
            volume=d("0.1"),
            price=d(0),
            ts=T0,
        )
    Position(
        position_id="p",
        symbol="EURUSD",
        side=Side.SELL,
        volume=d("0.1"),
        entry_price=d("1.1"),
        opened_ts=T0,
        strategy_id="manual",
        sl=d("1.2"),
    )
    with pytest.raises(ValueError, match="sl"):
        Position(
            position_id="p",
            symbol="EURUSD",
            side=Side.SELL,
            volume=d("0.1"),
            entry_price=d("1.1"),
            opened_ts=T0,
            strategy_id="manual",
            sl=d(-1),
        )
    AccountState(
        account_id="a",
        currency="USD",
        balance=d(100),
        equity=d(90),
        margin=d(10),
        free_margin=d(80),
        leverage=100,
        ts=datetime.now(UTC),
    )
    with pytest.raises(ValueError, match="leverage"):
        AccountState(
            account_id="a",
            currency="USD",
            balance=d(100),
            equity=d(90),
            margin=d(10),
            free_margin=d(80),
            leverage=0,
            ts=T0,
        )
