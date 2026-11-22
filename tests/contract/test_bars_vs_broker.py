"""Phase 4 acceptance (SPEC §16): bars built from our tick stream match the broker's candles for the same
window, within the documented tolerance (marketdata/compare.py, docs/adr/0006-market-data.md).

- paper: S1 bars over a few seconds, every run (plumbing; the fake broker builds candles from the same ticks,
  so they must match exactly).
- ctrader-demo / exness-demo (``-m demo``): M1 bars over ``TRADER_BAR_COMPARE_MINUTES`` (default 6) minutes,
  plus a report of the broker's D1 alignment (``MarketDataConfig.day_offset_minutes``).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from trader.domain import TickEvent, Timeframe
from trader.marketdata.bars import BarBuilder, PriceSource, bar_start
from trader.marketdata.compare import compare_bars, infer_day_offset

from .harness import HARNESSES, ContractHarness

TARGETS = [
    pytest.param("paper", id="paper"),
    pytest.param("ctrader-demo", id="ctrader-demo", marks=pytest.mark.demo),
    pytest.param("exness-demo", id="exness-demo", marks=pytest.mark.demo),
]


@pytest.fixture(params=TARGETS)
async def target(request: pytest.FixtureRequest) -> AsyncIterator[tuple[str, ContractHarness]]:
    h = await HARNESSES[request.param]()
    try:
        yield request.param, h
    finally:
        await h.cleanup()


async def test_built_bars_match_broker_candles(target: tuple[str, ContractHarness]) -> None:
    name, h = target
    paper = name == "paper"
    tf = Timeframe.S1 if paper else Timeframe.M1
    duration = tf.duration
    assert duration is not None
    window = (
        timedelta(seconds=4)
        if paper
        else timedelta(minutes=int(os.environ.get("TRADER_BAR_COMPARE_MINUTES", "6")))
    )
    inst = await h.instrument()

    mark = h.recorder.mark
    began = datetime.now(UTC)
    await asyncio.sleep(window.total_seconds())
    # Let the last full bar close on the broker side before asking for candles.
    ended = bar_start(datetime.now(UTC), tf)
    await asyncio.sleep(
        max(0.0, (ended + duration - datetime.now(UTC)).total_seconds()) + (0.2 if paper else 3)
    )

    builder = BarBuilder([tf], price=PriceSource.BID, grace=timedelta(0))
    ticks = [e.tick for e in h.recorder.of(TickEvent, after=mark) if e.tick.symbol == h.symbol]
    assert ticks, "no ticks received"
    for t in ticks:
        builder.on_tick(t)
    builder.on_time(datetime.now(UTC) + duration)
    built = [
        b
        for b in builder.history(h.symbol, tf)
        if b.start >= bar_start(began, tf) + duration and b.start < ended
    ]

    broker = await h.broker.candles(h.symbol, tf, bar_start(began, tf) + duration, ended)
    cmp = compare_bars(built, broker, tick_size=inst.tick_size, trim_edges=False)
    print(
        f"\n[bars-vs-broker:{name}] {tf} ticks={len(ticks)} built={len(built)} broker={len(broker)} "
        f"-> {cmp.summary()}"
    )
    for d in cmp.matched:
        if d.max_pts:
            print(f"  {d.start:%H:%M:%S} ΔO={d.open_pts} ΔH={d.high_pts} ΔL={d.low_pts} ΔC={d.close_pts}")
    if paper:
        assert cmp.within == len(cmp.matched)
        assert not cmp.missing
        assert not cmp.extra
        assert all(d.max_pts == 0 for d in cmp.matched)
    assert cmp.passes(min_ratio=Decimal("0.9"), max_missing=1), cmp.summary()

    if not paper:
        days = await h.broker.candles(
            h.symbol, Timeframe.D1, datetime.now(UTC) - timedelta(days=12), datetime.now(UTC)
        )
        offset = infer_day_offset([b.start for b in days])
        print(
            f"[bars-vs-broker:{name}] D1 starts {[f'{b.start:%a %H:%M}' for b in days[-5:]]} "
            f"-> day_offset_minutes={int(offset.total_seconds() // 60)}"
        )
