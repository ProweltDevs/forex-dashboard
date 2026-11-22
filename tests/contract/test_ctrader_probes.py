"""DEMO-ONLY probes for cTrader behaviour the docs do not state (CLAUDE.md: probe, don't guess).

Run with:  uv run pytest -m demo tests/contract/test_ctrader_probes.py -s
Each probe prints what it observed; record the results in docs/brokers/ctrader.md.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trader.domain import FillEvent, OrderType, PositionChange, PositionEvent, Side, Timeframe
from trader.domain.errors import BrokerError

from .harness import ContractHarness, ctrader_demo_harness

pytestmark = pytest.mark.demo


@pytest.fixture
async def demo() -> object:
    h = await ctrader_demo_harness()
    try:
        yield h
    finally:
        await h.cleanup()


async def test_probe_commission_sign_and_unrealized(demo: ContractHarness) -> None:
    inst = await demo.instrument()
    req = demo.request(Side.BUY, inst.min_volume)
    await demo.broker.place_order(req)
    fill = (await demo.fill_for(req.client_order_id)).fill
    positions = await demo.broker.positions()
    pos = next(p for p in positions if p.position_id == fill.position_id)
    account = await demo.broker.account()
    print(f"\n[probe] deal commission={fill.commission} position commission={pos.commission} swap={pos.swap}")
    print(f"[probe] balance={account.balance} equity={account.equity} unrealized(net)={pos.unrealized_pnl}")
    await demo.broker.close_position(fill.position_id)
    close = await demo.recorder.wait_for(
        FillEvent, lambda e: e.fill.position_id == fill.position_id and e.fill.realized_pnl is not None
    )
    print(f"[probe] close realized_pnl={close.fill.realized_pnl} commission={close.fill.commission}")
    assert fill.commission <= 0, "expected commission to be reported as a debit (<= 0)"


async def test_probe_clear_sl_by_omission(demo: ContractHarness) -> None:
    inst = await demo.instrument()
    tick = await demo.last_tick()
    req = demo.request(Side.BUY, inst.min_volume)
    await demo.broker.place_order(req)
    pid = (await demo.fill_for(req.client_order_id)).fill.position_id
    await demo.broker.modify_position(pid, inst.round_price(tick.bid * Decimal("0.98")), None)
    await demo.recorder.wait_for(
        PositionEvent, lambda e: e.position.position_id == pid and e.change is PositionChange.MODIFIED
    )
    await demo.broker.modify_position(pid, None, None)
    await demo.recorder.wait_for(
        PositionEvent, lambda e: e.position.position_id == pid and e.position.sl is None, timeout=10
    )
    pos = next(p for p in await demo.broker.positions() if p.position_id == pid)
    print(f"\n[probe] after amend without stopLoss: sl={pos.sl} tp={pos.tp}")
    assert pos.sl is None, "omitting stopLoss in AmendPositionSLTPReq is expected to clear it"


async def test_probe_trendbar_range_limits(demo: ContractHarness) -> None:
    """Find the largest single-request range per timeframe (the adapter halves on INCORRECT_BOUNDARIES)."""
    tick = await demo.last_tick()
    for tf in (Timeframe.M1, Timeframe.M5, Timeframe.H1, Timeframe.D1):
        duration = tf.duration
        assert duration is not None
        for bars in (500, 1000, 2000, 5000):
            try:
                got = await demo.broker.candles(demo.symbol, tf, tick.ts - duration * bars, tick.ts)
                print(f"[probe] {tf} {bars} bars requested -> {len(got)} returned")
            except BrokerError as exc:
                print(f"[probe] {tf} {bars} bars -> {exc.broker_code}")
                break


async def test_probe_stop_limit_round_trip(demo: ContractHarness) -> None:
    inst = await demo.instrument()
    tick = await demo.last_tick()
    stop = inst.round_price(tick.ask * Decimal("1.03"))
    limit = stop + inst.tick_size * 20
    req = demo.request(
        Side.BUY, inst.min_volume, order_type=OrderType.STOP_LIMIT, stop_price=stop, limit_price=limit
    )
    ack = await demo.broker.place_order(req)
    order = next(o for o in await demo.broker.open_orders() if o.client_order_id == req.client_order_id)
    print(
        f"\n[probe] STOP_LIMIT sent stop={stop} limit={limit}; "
        f"broker reports stop={order.request.stop_price} "
        f"limit={order.request.limit_price}"
    )
    assert ack.broker_order_id
    await demo.broker.cancel_order(ack.broker_order_id)


async def test_probe_order_history_lookup_after_restart(demo: ContractHarness) -> None:
    """A new adapter instance (no local links) must find a filled market order by clientOrderId through
    ProtoOAOrderListReq. The OMS resends on a miss and cTrader does not de-duplicate, so a miss here
    means a double order after a restart."""
    from datetime import UTC, datetime, timedelta

    inst = await demo.instrument()
    since = datetime.now(UTC) - timedelta(minutes=1)
    req = demo.request(Side.BUY, inst.min_volume)
    await demo.broker.place_order(req)
    fill = (await demo.fill_for(req.client_order_id)).fill  # demo.cleanup closes this position
    restarted = await ctrader_demo_harness()
    try:
        found = await restarted.broker.find_order(req.client_order_id, since)
        print(
            f"\n[probe] history lookup: status={found.status if found else None} "
            f"order_id={found.broker_order_id if found else None} (fill order {fill.broker_order_id}) "
            f"strategy={found.request.strategy_id if found else None}"
        )
        assert found is not None, "filled order not found in ProtoOAOrderListRes by clientOrderId"
        assert found.broker_order_id == fill.broker_order_id
    finally:
        await restarted.cleanup()
