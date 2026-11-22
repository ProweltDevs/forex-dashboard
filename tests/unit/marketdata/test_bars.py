"""Bar boundaries and aggregation, checked against an independent reference aggregator."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from trader.domain import Bar, Tick, Timeframe
from trader.marketdata.bars import BarBuilder, PriceSource, bar_end, bar_start

D = Decimal
T0 = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)  # Monday
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

times = st.datetimes(
    min_value=datetime(2000, 1, 1),  # noqa: DTZ001 - hypothesis bounds are naive
    max_value=datetime(2100, 1, 1),  # noqa: DTZ001
    timezones=st.just(UTC),
)
offsets = st.sampled_from(
    [timedelta(0), timedelta(hours=22), timedelta(hours=-2), timedelta(hours=2), timedelta(hours=3)]
)


def tick(ts: datetime, bid: str, ask: str | None = None, symbol: str = "EURUSD") -> Tick:
    b = D(bid)
    return Tick(
        symbol=symbol, bid=b, ask=D(ask) if ask else b + D("0.0001"), ts=ts, recv_ts=ts, recv_mono_ns=0
    )


# ------------------------------------------------------------------ boundaries


@given(times, st.sampled_from(list(Timeframe)), offsets)
def test_bar_start_contains_ts_and_is_idempotent(ts: datetime, tf: Timeframe, off: timedelta) -> None:
    start = bar_start(ts, tf, off)
    end = bar_end(start, tf, off)
    assert start <= ts < end
    assert bar_start(start, tf, off) == start
    assert bar_start(end, tf, off) == end  # the end is the next bar's start
    shifted = start - off
    if tf is Timeframe.MN1:
        assert (shifted.day, shifted.hour, shifted.minute) == (1, 0, 0)
    elif tf is Timeframe.W1:
        assert shifted.weekday() == 0
        assert (shifted.hour, shifted.minute, shifted.second) == (0, 0, 0)
    else:
        duration = tf.duration
        assert duration is not None
        assert (shifted - EPOCH) % duration == timedelta(0)


def test_known_boundaries() -> None:
    ts = datetime(2026, 3, 18, 21, 59, 59, 999000, tzinfo=UTC)  # Wednesday
    assert bar_start(ts, Timeframe.M5) == datetime(2026, 3, 18, 21, 55, tzinfo=UTC)
    assert bar_start(ts, Timeframe.H4) == datetime(2026, 3, 18, 20, tzinfo=UTC)
    assert bar_start(ts, Timeframe.D1) == datetime(2026, 3, 18, tzinfo=UTC)
    ny = timedelta(hours=22)  # "NY close" trading day
    assert bar_start(ts, Timeframe.D1, ny) == datetime(2026, 3, 17, 22, tzinfo=UTC)
    assert bar_start(ts + timedelta(milliseconds=1), Timeframe.D1, ny) == datetime(
        2026, 3, 18, 22, tzinfo=UTC
    )
    assert bar_start(ts, Timeframe.W1) == datetime(2026, 3, 16, tzinfo=UTC)
    assert bar_start(ts, Timeframe.MN1) == datetime(2026, 3, 1, tzinfo=UTC)
    assert bar_end(datetime(2026, 12, 1, tzinfo=UTC), Timeframe.MN1) == datetime(2027, 1, 1, tzinfo=UTC)


# ------------------------------------------------------------------ aggregation vs reference


def reference(
    ticks: list[Tick], tf: Timeframe, off: timedelta, price: PriceSource
) -> list[tuple[object, ...]]:
    out = []
    for start, group in itertools.groupby(ticks, key=lambda t: bar_start(t.ts, tf, off)):
        prices = [price.of(t) for t in group]
        out.append((start, prices[0], max(prices), min(prices), prices[-1], D(len(prices))))
    return out


def as_tuple(b: Bar) -> tuple[object, ...]:
    return (b.start, b.open, b.high, b.low, b.close, b.volume)


tick_streams = st.lists(
    st.tuples(st.integers(min_value=0, max_value=5_000), st.integers(min_value=-50, max_value=50)),
    min_size=1,
    max_size=200,
)


@settings(max_examples=150)
@given(
    tick_streams,
    st.sampled_from([Timeframe.S5, Timeframe.M1, Timeframe.M5]),
    offsets,
    st.sampled_from(list(PriceSource)),
)
def test_builder_matches_reference(
    steps: list[tuple[int, int]], tf: Timeframe, off: timedelta, price: PriceSource
) -> None:
    ts, px = T0, D("1.10000")
    ticks = []
    for dt_ms, dpx in steps:
        ts += timedelta(milliseconds=dt_ms)
        px += D(dpx) * D("0.00001")
        ticks.append(tick(ts, str(px), str(px + D("0.00008"))))
    builder = BarBuilder([tf], price=price, day_offset=off)
    completed: list[Bar] = []
    for t in ticks:
        completed.extend(builder.on_tick(t))
    current = builder.current("EURUSD", tf)
    assert current is not None
    assert not current.complete
    built = [as_tuple(b) for b in completed] + [as_tuple(current)]
    assert built == reference(ticks, tf, off, price)
    assert all(b.complete for b in completed)
    assert builder.history("EURUSD", tf) == completed


def test_multi_timeframe_and_timer_close() -> None:
    builder = BarBuilder([Timeframe.M1, Timeframe.M5], grace=timedelta(seconds=2))
    assert builder.on_tick(tick(T0, "1.1")) == []
    assert builder.on_tick(tick(T0 + timedelta(seconds=30), "1.2")) == []
    done = builder.on_tick(tick(T0 + timedelta(minutes=1), "1.15"))  # exact boundary -> new M1 bar
    assert [(b.timeframe, b.start, b.high, b.close, b.volume) for b in done] == [
        (Timeframe.M1, T0, D("1.2"), D("1.2"), D(2))
    ]
    assert builder.on_time(T0 + timedelta(minutes=2, seconds=1)) == []  # within grace
    closed = builder.on_time(T0 + timedelta(minutes=2, seconds=2))
    assert [(b.timeframe, b.start) for b in closed] == [(Timeframe.M1, T0 + timedelta(minutes=1))]
    assert builder.current("EURUSD", Timeframe.M1) is None  # no empty bars for silence
    assert builder.current("EURUSD", Timeframe.M5) is not None
    closed = builder.on_time(T0 + timedelta(minutes=5, seconds=2))
    assert [(b.timeframe, b.volume) for b in closed] == [(Timeframe.M5, D(3))]


def test_late_ticks_are_ignored_and_counted() -> None:
    builder = BarBuilder([Timeframe.M1])
    builder.on_tick(tick(T0 + timedelta(minutes=1), "1.1"))
    builder.on_tick(tick(T0 + timedelta(seconds=59), "9.9"))  # older than the forming bar
    assert builder.late_ticks == 1
    builder.on_time(T0 + timedelta(minutes=3))
    builder.on_tick(tick(T0 + timedelta(minutes=1, seconds=5), "9.9"))  # belongs to an already-closed bar
    assert builder.late_ticks == 2
    assert builder.history("EURUSD", Timeframe.M1)[-1].high == D("1.1")


def test_seed_from_broker_candles_then_continue() -> None:
    def bar(minute: int, complete: bool, close: str = "1.1") -> Bar:
        c = D(close)
        return Bar(
            symbol="EURUSD",
            timeframe=Timeframe.M1,
            start=T0 + timedelta(minutes=minute),
            open=c,
            high=c,
            low=c,
            close=c,
            volume=D(5),
            complete=complete,
        )

    builder = BarBuilder([Timeframe.M1], history=3)
    builder.seed([bar(2, False, "1.3"), bar(0, True), bar(1, True), bar(1, True, "1.2"), bar(-1, True)])
    hist = builder.history("EURUSD", Timeframe.M1)
    assert [b.start for b in hist] == [T0 - timedelta(minutes=1), T0, T0 + timedelta(minutes=1)]
    assert hist[-1].close == D("1.2")  # newer duplicate wins
    forming = builder.current("EURUSD", Timeframe.M1)
    assert forming is not None
    assert forming.close == D("1.3")
    done = builder.on_tick(tick(T0 + timedelta(minutes=2, seconds=10), "1.35"))
    assert done == []
    done = builder.on_tick(tick(T0 + timedelta(minutes=3), "1.4"))
    assert [(b.start, b.open, b.close, b.volume) for b in done] == [
        (T0 + timedelta(minutes=2), D("1.3"), D("1.35"), D(6))
    ]
    assert len(builder.history("EURUSD", Timeframe.M1)) == 3  # bounded


def test_symbols_are_independent_and_validation() -> None:
    builder = BarBuilder([Timeframe.M1])
    builder.on_tick(tick(T0, "1.1"))
    builder.on_tick(tick(T0, "2000", symbol="XAUUSD"))
    done = builder.on_tick(tick(T0 + timedelta(minutes=1), "1.2"))
    assert [b.symbol for b in done] == ["EURUSD"]
    with pytest.raises(ValueError, match="timeframe"):
        BarBuilder([])
