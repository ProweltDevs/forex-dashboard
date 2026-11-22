from __future__ import annotations

import asyncio
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal

import pytest

from tests.factories import eurusd
from trader.domain import FeedState, Tick, TickEvent, TradingSession
from trader.marketdata.bus import Bus, Overflow, TickBus
from trader.marketdata.stale import StaleFeedMonitor

T0 = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)  # Monday


def ev(symbol: str = "EURUSD", ts: datetime = T0, bid: str = "1.1", broker: str = "paper") -> TickEvent:
    b = Decimal(bid)
    return TickEvent(tick=Tick(symbol=symbol, bid=b, ask=b, ts=ts, recv_ts=ts, recv_mono_ns=0), broker=broker)


# ------------------------------------------------------------------ bus


async def test_lossless_never_drops_and_fans_out() -> None:
    bus: Bus[int] = Bus()
    a = bus.subscribe("oms", maxsize=2)
    b = bus.subscribe("recorder", maxsize=100)
    for i in range(10):
        bus.publish(i)
    assert [a.get_nowait() for _ in range(10)] == list(range(10))
    assert a.dropped == 0
    assert len(b) == 10


def test_drop_oldest_and_coalesce() -> None:
    bus: Bus[tuple[str, int]] = Bus()
    drop = bus.subscribe("drop", policy=Overflow.DROP_OLDEST, maxsize=3)
    co = bus.subscribe("ui", policy=Overflow.COALESCE, maxsize=10, key=lambda x: x[0])
    for i in range(5):
        bus.publish(("EURUSD", i))
    bus.publish(("XAUUSD", 9))
    bus.publish(("EURUSD", 7))
    assert [drop.get_nowait() for _ in range(3)] == [("EURUSD", 4), ("XAUUSD", 9), ("EURUSD", 7)]
    assert drop.dropped == 4
    assert co.get_nowait() == ("XAUUSD", 9)  # EURUSD was updated after XAUUSD, so it moved to the back
    assert co.get_nowait() == ("EURUSD", 7)
    assert co.get_nowait() is None
    assert co.dropped == 5
    with pytest.raises(ValueError, match="key"):
        bus.subscribe("x", policy=Overflow.COALESCE)


async def test_async_iteration_filter_and_close() -> None:
    bus: Bus[int] = Bus()
    sub = bus.subscribe("evens", accept=lambda x: x % 2 == 0)
    got: list[int] = []

    async def consume() -> None:
        async for item in sub:
            got.append(item)

    task = asyncio.create_task(consume())
    for i in range(6):
        bus.publish(i)
        await asyncio.sleep(0)
    bus.unsubscribe(sub)
    await asyncio.wait_for(task, 1)
    assert got == [0, 2, 4]
    bus.publish(8)
    assert got == [0, 2, 4]


def test_tick_bus_last_cache_and_out_of_order() -> None:
    bus = TickBus()
    sub = bus.subscribe("s")
    ui = bus.coalesced("ui")
    bus.publish(ev(ts=T0, bid="1.1"))
    bus.publish(ev(ts=T0 + timedelta(seconds=1), bid="1.2"))
    bus.publish(ev(ts=T0, bid="0.9"))  # out of order: dropped everywhere
    bus.publish(ev(ts=T0, bid="1.0", broker="ctrader"))
    last = bus.last("EURUSD", "paper")
    assert last is not None
    assert last.bid == Decimal("1.2")
    any_broker = bus.last("EURUSD")
    assert any_broker is not None
    assert any_broker.bid == Decimal("1.0")
    assert len(sub) == 3
    assert len(ui) == 2  # one per (broker, symbol)


# ------------------------------------------------------------------ stale monitor


def tick_at(ts: datetime) -> Tick:
    return Tick(symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1001"), ts=ts, recv_ts=ts, recv_mono_ns=0)


def test_stale_after_threshold_and_live_on_next_tick() -> None:
    mon = StaleFeedMonitor(
        "paper",
        min_silence=timedelta(seconds=5),
        max_silence=timedelta(seconds=60),
        multiplier=10,
        warmup_samples=5,
    )
    mon.watch(eurusd(), T0)
    ts = T0
    for _ in range(30):  # a tick every 200 ms -> threshold clamps to min_silence (5 s)
        ts += timedelta(milliseconds=200)
        assert mon.observe(tick_at(ts)) is None
    assert mon.threshold("EURUSD") == timedelta(seconds=5)
    assert mon.check(ts + timedelta(seconds=4)) == []
    events = mon.check(ts + timedelta(seconds=6))
    assert [(e.state, e.symbol) for e in events] == [(FeedState.STALE, "EURUSD")]
    assert events[0].silence_ms == 6000
    assert mon.check(ts + timedelta(seconds=30)) == []  # reported once
    live = mon.observe(tick_at(ts + timedelta(seconds=31)))
    assert live is not None
    assert live.state is FeedState.LIVE
    assert mon.state("EURUSD") is FeedState.LIVE


def test_threshold_adapts_to_slow_symbols_and_caps() -> None:
    mon = StaleFeedMonitor(
        "p",
        min_silence=timedelta(seconds=5),
        max_silence=timedelta(seconds=60),
        multiplier=10,
        warmup_samples=3,
    )
    mon.watch(eurusd(), T0)
    assert mon.threshold("EURUSD") == timedelta(seconds=60)  # warming up
    ts = T0
    for _ in range(10):
        ts += timedelta(seconds=3)
        mon.observe(tick_at(ts))
    assert mon.threshold("EURUSD") == timedelta(seconds=30)
    for _ in range(20):
        ts += timedelta(seconds=50)
        mon.observe(tick_at(ts))
    assert mon.threshold("EURUSD") == timedelta(seconds=60)  # capped at max_silence


def test_no_stale_alert_while_session_closed_or_never_watched() -> None:
    weekday_only = eurusd(
        sessions=tuple(TradingSession(weekday=d, start=time(0), end=time(0)) for d in range(5))
    )
    mon = StaleFeedMonitor("p", min_silence=timedelta(seconds=5), max_silence=timedelta(seconds=10))
    saturday = datetime(2026, 1, 10, 12, tzinfo=UTC)
    mon.watch(weekday_only, saturday)
    assert mon.check(saturday + timedelta(hours=5)) == []
    monday = datetime(2026, 1, 12, 0, 0, 30, tzinfo=UTC)
    assert [e.state for e in mon.check(monday)] == [FeedState.STALE]  # silent since it was watched
    assert (
        mon.observe(Tick(symbol="GBPUSD", bid=Decimal(1), ask=Decimal(1), ts=T0, recv_ts=T0, recv_mono_ns=0))
        is None
    )
    with pytest.raises(ValueError, match="min_silence"):
        StaleFeedMonitor("p", min_silence=timedelta(seconds=10), max_silence=timedelta(seconds=5))
