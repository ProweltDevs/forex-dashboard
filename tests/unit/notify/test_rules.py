from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trader.domain.enums import (
    ConnectionState,
    FeedState,
    KillLevel,
    OrderStatus,
    OrderType,
    PositionChange,
    Side,
)
from trader.domain.events import (
    AlertEvent,
    ConnectionEvent,
    FeedEvent,
    FillEvent,
    OrderEvent,
    PositionEvent,
    ReconcileEvent,
    RiskStateEvent,
    StrategyEvent,
)
from trader.domain.models import Fill, Order, OrderRequest, Position, StatusChange
from trader.notify.notifier import Severity
from trader.notify.rules import EventRules, classify_exit

T = datetime(2026, 3, 2, 9, 0, tzinfo=UTC)
A = "acct"
D = Decimal


def pos(side: Side = Side.BUY, sl: str | None = "1.0950", tp: str | None = "1.1100") -> Position:
    return Position(
        position_id="P1",
        symbol="EURUSD",
        side=side,
        volume=D("0.10"),
        entry_price=D("1.1000"),
        opened_ts=T,
        strategy_id="ema",
        sl=None if sl is None else D(sl),
        tp=None if tp is None else D(tp),
    )


@pytest.mark.parametrize(
    ("side", "sl", "tp", "price", "expected"),
    [
        (Side.BUY, "1.0950", "1.1100", "1.0950", "SL"),
        (Side.BUY, "1.0950", "1.1100", "1.0940", "SL"),  # slipped through the stop
        (Side.BUY, "1.0950", "1.1100", "1.0954", "SL"),  # within 10% of the stop distance
        (Side.BUY, "1.0950", "1.1100", "1.0960", None),
        (Side.BUY, "1.0950", "1.1100", "1.1100", "TP"),
        (Side.BUY, "1.0950", "1.1100", "1.1092", "TP"),
        (Side.BUY, "1.0950", "1.1100", "1.1080", None),
        (Side.SELL, "1.1050", "1.0900", "1.1050", "SL"),
        (Side.SELL, "1.1050", "1.0900", "1.0890", "TP"),
        (Side.SELL, "1.1050", "1.0900", "1.1000", None),
        (Side.BUY, None, None, "1.0000", None),
    ],
)
def test_classify_exit(side: Side, sl: str | None, tp: str | None, price: str, expected: str | None) -> None:
    assert classify_exit(pos(side, sl, tp), D(price)) == expected


def fill(fill_id: str, side: Side, price: str, pnl: str | None = None) -> FillEvent:
    return FillEvent(
        account_id=A,
        fill=Fill(
            fill_id=fill_id,
            client_order_id=None,
            broker_order_id="B1",
            position_id="P1",
            symbol="EURUSD",
            side=side,
            volume=D("0.10"),
            price=D(price),
            ts=T,
            commission=D("-0.70"),
            realized_pnl=None if pnl is None else D(pnl),
        ),
        ts=T,
    )


def test_fills_sl_hits_and_closes() -> None:
    rules = EventRules()
    opened = rules.notice(fill("F1", Side.BUY, "1.1000"))
    assert opened is not None
    assert opened.title == "Fill BUY EURUSD"
    assert (
        rules.notice(PositionEvent(account_id=A, position=pos(), change=PositionChange.OPENED, ts=T)) is None
    )
    hit = rules.notice(fill("F2", Side.SELL, "1.0949", pnl="-51.00"))
    assert hit is not None
    assert hit.title == "SL hit EURUSD"
    assert "P&L -51.70" in hit.text  # net of commission
    assert hit.key == "fill:F2"
    rules.notice(PositionEvent(account_id=A, position=pos(), change=PositionChange.CLOSED, ts=T))
    again = rules.notice(fill("F3", Side.SELL, "1.0949"))  # position gone: a plain fill
    assert again is not None
    assert again.title == "Fill SELL EURUSD"


def test_no_fill_notices_when_disabled_but_exits_still_notify() -> None:
    rules = EventRules(fills=False)
    assert rules.notice(fill("F1", Side.BUY, "1.1000")) is None
    rules.notice(PositionEvent(account_id=A, position=pos(), change=PositionChange.OPENED, ts=T))
    tp = rules.notice(fill("F2", Side.SELL, "1.1100"))
    assert tp is not None
    assert tp.title == "TP hit EURUSD"


def order_event(status: OrderStatus, reason: str | None, previous: OrderStatus | None = None) -> OrderEvent:
    req = OrderRequest(
        client_order_id="CID-0001",
        symbol="EURUSD",
        side=Side.BUY,
        order_type=OrderType.MARKET,
        volume=D("0.10"),
        strategy_id="ema",
        strategy_version="1",
    )
    order = Order(
        request=req, status=status, history=(StatusChange(status=status, ts=T),), rejection_reason=reason
    )
    return OrderEvent(account_id=A, order=order, previous_status=previous, ts=T)


def test_rejects_are_keyed_by_rule() -> None:
    rules = EventRules()
    n = rules.notice(order_event(OrderStatus.REJECTED, "risk:max_positions_symbol: 3 open (limit 3)"))
    assert n is not None
    assert n.key == "reject:acct:ema:risk:max_positions_symbol"
    assert n.severity is Severity.WARNING
    assert rules.notice(order_event(OrderStatus.FILLED, None)) is None
    assert rules.notice(order_event(OrderStatus.REJECTED, "x", previous=OrderStatus.REJECTED)) is None


def test_risk_connection_feed_strategy_reconcile_alert() -> None:
    rules = EventRules()

    def risk(level: KillLevel, prev: KillLevel) -> RiskStateEvent:
        return RiskStateEvent(
            account_id=A, level=level, previous=prev, reason="r", triggers=("daily_loss",), ts=T
        )

    up = rules.notice(risk(KillLevel.FLATTEN_ALL, KillLevel.NONE))
    assert up is not None
    assert (up.severity, up.title) == (Severity.CRITICAL, "Kill switch FLATTEN_ALL")
    down = rules.notice(risk(KillLevel.PAUSE_NEW, KillLevel.FLATTEN_ALL))
    assert down is not None
    assert down.severity is Severity.WARNING
    cleared = rules.notice(risk(KillLevel.NONE, KillLevel.PAUSE_NEW))
    assert cleared is not None
    assert cleared.title == "Risk cleared"

    def conn(state: ConnectionState) -> ConnectionEvent:
        return ConnectionEvent(account_id=A, broker="paper", state=state, ts=T, reason="net")

    assert rules.notice(conn(ConnectionState.UP)) is None  # first connect
    lost = rules.notice(conn(ConnectionState.DOWN))
    assert lost is not None
    assert lost.title == "Disconnected"
    back = rules.notice(conn(ConnectionState.UP))
    assert back is not None
    assert back.title == "Reconnected"

    def feed(state: FeedState) -> FeedEvent:
        return FeedEvent(broker=A, symbol="EURUSD", state=state, ts=T, last_tick_ts=T, silence_ms=30_000)

    assert rules.notice(feed(FeedState.LIVE)) is None  # it was never stale
    stale = rules.notice(feed(FeedState.STALE))
    assert stale is not None
    assert "no ticks for 30 s" in stale.text
    live = rules.notice(feed(FeedState.LIVE))
    assert live is not None
    assert live.title == "Feed back EURUSD"

    err = rules.notice(
        StrategyEvent(strategy_id="ema", kind="error", message="on_bar: ZeroDivisionError", ts=T)
    )
    assert err is not None
    assert err.key == "strategy_error:ema"
    auto = rules.notice(StrategyEvent(strategy_id="ema", kind="paused", message="5 errors (last: x)", ts=T))
    assert auto is not None
    for msg in ("kill switch PAUSE_NEW: manual", "shutdown"):
        assert rules.notice(StrategyEvent(strategy_id="ema", kind="paused", message=msg, ts=T)) is None
    assert rules.notice(StrategyEvent(strategy_id="ema", kind="started", message="running", ts=T)) is None

    rec = rules.notice(
        ReconcileEvent(
            account_id=A,
            kind="position_adopted",
            entity_id="P9",
            before=None,
            after="BUY 1",
            reason="r",
            ts=T,
        )
    )
    assert rec is not None
    assert rec.severity is Severity.WARNING
    db = rules.notice(AlertEvent(account_id="*", kind="db_down", severity="critical", message="m", ts=T))
    assert db is not None
    assert (db.severity, db.title, db.account_id) == (Severity.CRITICAL, "db down", None)


prices = st.decimals(min_value=D("0.5"), max_value=D("2"), places=5)
gaps = st.decimals(min_value=D("0.0005"), max_value=D("0.05"), places=5)


@given(entry=prices, stop=gaps, target=gaps, side=st.sampled_from(Side))
def test_exits_at_the_levels_are_classified_for_both_sides(
    entry: Decimal, stop: Decimal, target: Decimal, side: Side
) -> None:
    sign = 1 if side is Side.BUY else -1
    p = Position(
        position_id="P",
        symbol="X",
        side=side,
        volume=D("1"),
        entry_price=entry,
        opened_ts=T,
        strategy_id="s",
        sl=entry - sign * stop,
        tp=entry + sign * target,
    )
    assert classify_exit(p, entry - sign * stop) == "SL"
    assert classify_exit(p, entry - sign * stop * 2) == "SL"  # slipped
    assert classify_exit(p, entry + sign * target) == "TP"
    assert classify_exit(p, entry) is None  # flat at entry: neither
