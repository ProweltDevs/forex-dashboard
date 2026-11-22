"""DEMO-ONLY probes for Exness behaviour the docs leave open (CLAUDE.md: probe, don't guess).

Run with:  uv run pytest -m demo tests/contract/test_exness_probes.py -s
Requires EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes (Exness does not separate demo and real accounts by host).
Each probe prints what it observed; record the results in docs/brokers/exness.md.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from trader.domain import FillEvent, PositionChange, PositionEvent, Side, TickEvent

from .harness import ContractHarness, exness_demo_harness

pytestmark = pytest.mark.demo


@pytest.fixture
async def demo() -> AsyncIterator[ContractHarness]:
    h = await exness_demo_harness()
    try:
        yield h
    finally:
        await h.cleanup()


async def test_probe_streams_limits_and_naming(demo: ContractHarness) -> None:
    """WebSocket URL/subprotocol (the docs mark it 'not yet verified'), tick timestamps, naming."""
    tick = (await demo.recorder.wait_for(TickEvent)).tick
    inst = await demo.instrument()
    limits = await demo.broker.limits()
    print(f"\n[probe] ticks stream OK; broker_ts={tick.broker_ts!r} (None = no parseable timestamp)")
    print(f"[probe] instrument {inst.symbol} -> broker name {inst.broker_symbol!r}, digits={inst.digits}")
    print(f"[probe] limits: {[(b.name, b.burst, str(b.rate_per_s)) for b in limits.buckets][:8]} ...")
    print(f"[probe] max candles/request: {demo.broker.capabilities.max_candles_per_request}")


async def test_probe_sltp_visibility_and_hedging(demo: ContractHarness) -> None:
    """Are position SL/TP visible in the REST snapshot (as sltp orders)? Can two positions coexist?"""
    inst = await demo.instrument()
    tick = await demo.last_tick()
    sl = inst.round_price(tick.bid * Decimal("0.98"))
    first = demo.request(Side.BUY, inst.min_volume, sl=sl)
    second = demo.request(Side.SELL, inst.min_volume)
    await demo.broker.place_order(first)
    await demo.broker.place_order(second)
    f1 = (await demo.fill_for(first.client_order_id)).fill
    f2 = (await demo.fill_for(second.client_order_id)).fill
    positions = await demo.broker.positions()
    mine = [p for p in positions if p.position_id in (f1.position_id, f2.position_id)]
    print(
        f"\n[probe] open deal order_id present: {bool(f1.broker_order_id)}; "
        f"positions after BUY+SELL: {len(mine)}"
    )
    pos1 = next((p for p in mine if p.position_id == f1.position_id), None)
    print(f"[probe] SL sent={sl}; SL from REST snapshot={pos1.sl if pos1 else None}")
    assert len(mine) == 2, "expected a hedging account (two opposite positions)"


async def test_probe_clear_sltp(demo: ContractHarness) -> None:
    inst = await demo.instrument()
    tick = await demo.last_tick()
    req = demo.request(Side.BUY, inst.min_volume, sl=inst.round_price(tick.bid * Decimal("0.98")))
    await demo.broker.place_order(req)
    pid = (await demo.fill_for(req.client_order_id)).fill.position_id
    await demo.broker.modify_position(pid, None, None)
    ev = await demo.recorder.wait_for(
        PositionEvent,
        lambda e: e.position.position_id == pid and e.change is PositionChange.MODIFIED,
        timeout=10,
    )
    print(f"\n[probe] after cancelling the sltp order: sl={ev.position.sl} tp={ev.position.tp}")
    assert ev.position.sl is None


async def test_probe_close_deal_fields(demo: ContractHarness) -> None:
    inst = await demo.instrument()
    req = demo.request(Side.BUY, inst.min_volume)
    await demo.broker.place_order(req)
    pid = (await demo.fill_for(req.client_order_id)).fill.position_id
    await demo.broker.close_position(pid)
    close = await demo.recorder.wait_for(
        FillEvent, lambda e: e.fill.position_id == pid and e.fill.realized_pnl is not None
    )
    print(
        f"\n[probe] close deal: profit={close.fill.realized_pnl} "
        f"commission={close.fill.commission} swap={close.fill.swap}"
    )
