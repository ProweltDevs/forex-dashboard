from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from prometheus_client import generate_latest

from trader.domain.enums import ConnectionState, OrderStatus, OrderType, Side
from trader.domain.events import AlertEvent, ConnectionEvent, OrderEvent, TickEvent
from trader.domain.models import Order, OrderRequest, StatusChange, Tick
from trader.domain.time import FixedClock
from trader.observability.metrics import Metrics, OpsMonitor, reject_rule

T0 = datetime(2026, 3, 2, 9, 0, tzinfo=UTC)
A = "acct"


def monitor(clock: FixedClock) -> OpsMonitor:
    return OpsMonitor(None, Metrics(), clock)  # type: ignore[arg-type]  # observe() never touches the app


def value(m: Metrics, name: str, **labels: str) -> float | None:
    v = m.registry.get_sample_value(name, labels)
    return None if v is None else float(v)


def order(
    status: OrderStatus, history: list[tuple[OrderStatus, float]], reason: str | None = None
) -> OrderEvent:
    req = OrderRequest(
        client_order_id="CID-0001",
        symbol="EURUSD",
        side=Side.BUY,
        order_type=OrderType.MARKET,
        volume=Decimal("0.1"),
        strategy_id="ema",
        strategy_version="1",
    )
    changes = tuple(StatusChange(status=s, ts=T0 + timedelta(seconds=dt)) for s, dt in history)
    o = Order(request=req, status=status, history=changes, rejection_reason=reason)
    return OrderEvent(account_id=A, order=o, previous_status=None, ts=changes[-1].ts)


def test_order_latency_and_counts() -> None:
    mon = monitor(FixedClock(T0))
    steps = [(OrderStatus.NEW, 0.0), (OrderStatus.PENDING_SUBMIT, 0.001), (OrderStatus.ACCEPTED, 0.051)]
    mon.observe(order(OrderStatus.ACCEPTED, steps))
    mon.observe(order(OrderStatus.ACCEPTED, steps))  # the same status again: counted once
    mon.observe(order(OrderStatus.FILLED, [*steps, (OrderStatus.FILLED, 0.151)]))
    m = mon.metrics
    assert value(m, "trader_order_status_changes_total", account=A, strategy="ema", status="ACCEPTED") == 1
    assert value(m, "trader_order_status_changes_total", account=A, strategy="ema", status="FILLED") == 1
    assert value(m, "trader_order_latency_seconds_count", account=A, leg="send_to_ack") == 1
    assert (
        abs((value(m, "trader_order_latency_seconds_sum", account=A, leg="send_to_ack") or 0) - 0.05) < 1e-9
    )
    assert abs((value(m, "trader_order_latency_seconds_sum", account=A, leg="ack_to_fill") or 0) - 0.1) < 1e-9
    assert value(m, "trader_order_latency_seconds_bucket", account=A, leg="submit_to_fill", le="0.25") == 1


def test_rejects_by_rule() -> None:
    mon = monitor(FixedClock(T0))
    mon.observe(
        order(OrderStatus.REJECTED, [(OrderStatus.REJECTED, 0)], "risk:max_positions: 3 open (limit 3)")
    )
    assert (
        value(mon.metrics, "trader_order_rejects_total", account=A, strategy="ema", rule="risk:max_positions")
        == 1
    )
    assert reject_rule("TRADING_BAD_VOLUME: volume step") == "TRADING_BAD_VOLUME"
    assert reject_rule(None) == "rejected"


def test_disconnects_and_reconnects() -> None:
    mon = monitor(FixedClock(T0))
    for state in (ConnectionState.UP, ConnectionState.DOWN, ConnectionState.DOWN, ConnectionState.UP):
        mon.observe(ConnectionEvent(account_id=A, broker="ctrader", state=state, ts=T0))
    m = mon.metrics
    assert value(m, "trader_broker_disconnects_total", account=A) == 1
    assert value(m, "trader_broker_reconnects_total", account=A) == 1  # the first UP is a connect
    assert value(m, "trader_broker_connected", account=A) == 1


def tick(clock: FixedClock, broker_lag_s: float) -> TickEvent:
    now = clock.now()
    t = Tick(
        symbol="EURUSD",
        bid=Decimal("1.1"),
        ask=Decimal("1.1001"),
        ts=now - timedelta(seconds=broker_lag_s),
        recv_ts=now,
        recv_mono_ns=0,
        broker_ts=now - timedelta(seconds=broker_lag_s),
    )
    return TickEvent(tick=t, broker="ct")


def test_clock_drift_alerts_once_and_clears() -> None:
    clock = FixedClock(T0)
    mon = monitor(clock)
    alerts = mon.events.subscribe("t")
    for _ in range(5):  # the host clock is 3 s ahead of the broker's (plus ~0 latency)
        mon.observe(tick(clock, 3.0))
        clock.advance(timedelta(seconds=1))
    mon.check_clock(clock.now())
    mon.check_clock(clock.now())
    first = alerts.get_nowait()
    assert isinstance(first, AlertEvent)
    assert first.kind == "clock_drift"
    assert "+3.00 s" in first.message
    assert alerts.get_nowait() is None  # once
    assert value(mon.metrics, "trader_clock_offset_seconds", feed="ct") == 3.0
    clock.advance(timedelta(seconds=61))  # the old samples leave the window
    mon.observe(tick(clock, 0.02))
    mon.check_clock(clock.now())
    cleared = alerts.get_nowait()
    assert isinstance(cleared, AlertEvent)
    assert cleared.kind == "clock_drift_cleared"
    assert mon.clock_offsets() == {"ct": 0.02}


def test_metrics_text_format() -> None:
    m = Metrics()
    m.build.info({"version": "x", "mode": "paper"})
    text = generate_latest(m.registry).decode()
    assert 'trader_build_info{mode="paper",version="x"} 1.0' in text
