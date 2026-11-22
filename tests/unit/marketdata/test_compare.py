from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from trader.domain import Bar, Timeframe
from trader.marketdata.compare import compare_bars, infer_day_offset

D = Decimal
T0 = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)
PT = D("0.00001")


def bar(i: int, o: str, h: str, low: str, c: str) -> Bar:
    return Bar(
        symbol="EURUSD",
        timeframe=Timeframe.M1,
        start=T0 + timedelta(minutes=i),
        open=D(o),
        high=D(h),
        low=D(low),
        close=D(c),
        volume=D(1),
        complete=True,
    )


def test_identical_bars_pass_and_edges_are_trimmed() -> None:
    bars = [bar(i, "1.10000", "1.10010", "1.09990", "1.10005") for i in range(5)]
    cmp = compare_bars(bars, bars, tick_size=PT)
    assert len(cmp.matched) == 3  # first and last excluded
    assert cmp.passes()
    assert "matched=3" in cmp.summary()


def test_missed_extremes_within_tolerance_but_never_outside() -> None:
    broker = [bar(i, "1.10000", "1.10010", "1.09990", "1.10005") for i in range(5)]
    built = [bar(i, "1.10000", "1.10008", "1.09992", "1.10005") for i in range(5)]  # throttled: missed 2 pts
    cmp = compare_bars(built, broker, tick_size=PT)
    assert cmp.passes()
    assert max(d.max_pts for d in cmp.matched) == 2
    beyond = [
        bar(i, "1.10000", "1.10015", "1.09990", "1.10005") for i in range(5)
    ]  # above broker high: wrong side?
    bad = compare_bars(beyond, broker, tick_size=PT)
    assert bad.outside
    assert not bad.passes()


def test_misaligned_boundaries_fail() -> None:
    broker = [bar(i, "1.1", "1.1", "1.1", "1.1") for i in range(6)]
    shifted = [bar(i, "1.1", "1.1", "1.1", "1.1") for i in range(1, 7)]  # same grid, one bar later
    cmp = compare_bars(shifted, broker, tick_size=PT)
    assert cmp.passes()  # same grid, edges trimmed
    off_grid = [
        Bar(
            symbol="EURUSD",
            timeframe=Timeframe.M1,
            start=b.start + timedelta(seconds=30),
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
            complete=True,
        )
        for b in broker
    ]
    bad = compare_bars(off_grid, broker, tick_size=PT)
    assert bad.extra
    assert bad.missing
    assert not bad.passes()
    assert not compare_bars([], [], tick_size=PT).passes()


def test_infer_day_offset() -> None:
    utc = [datetime(2026, 1, d, tzinfo=UTC) for d in range(5, 10)]
    assert infer_day_offset(utc) == timedelta(0)
    ny = [datetime(2026, 1, d, 22, tzinfo=UTC) for d in range(4, 9)]
    assert infer_day_offset(ny) == timedelta(hours=-2)
    with pytest.raises(ValueError, match="D1"):
        infer_day_offset([])
