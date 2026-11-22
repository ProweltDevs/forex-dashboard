"""The broker contract (docs/SPEC.md §4). Uses only the ``Broker`` protocol, so the same tests run against
every adapter. A broker is not "supported" until this suite is green on its demo account."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from trader.brokers.base import Broker, OrderChanges
from trader.domain import (
    ConnectionEvent,
    ConnectionState,
    OrderEvent,
    OrderStatus,
    OrderType,
    PositionChange,
    PositionEvent,
    Side,
    TickEvent,
)
from trader.domain.errors import BrokerUnavailable, RateLimited
from trader.domain.events import FillEvent

from .harness import ContractHarness


async def test_connect_instruments_account(harness: ContractHarness) -> None:
    broker = harness.broker
    assert isinstance(broker, Broker)
    caps = broker.capabilities
    assert OrderType.MARKET in caps.order_types
    assert caps.max_candles_per_request > 0

    inst = await harness.instrument()
    assert inst.min_volume <= inst.max_volume
    assert inst.contract_size > 0

    account = await broker.account()
    assert account.currency
    assert account.equity >= 0
    assert account.ts.tzinfo is not None


async def test_tick_stream(harness: ContractHarness) -> None:
    start = harness.recorder.mark
    deadline = time.monotonic() + 3
    while len(harness.recorder.of(TickEvent, after=start)) < 5:
        assert time.monotonic() < deadline, "fewer than 5 ticks in 3 s"
        await asyncio.sleep(0.02)
    tick = harness.recorder.of(TickEvent, after=start)[-1].tick
    assert tick.symbol == harness.symbol
    assert 0 < tick.bid <= tick.ask
    assert tick.ts.tzinfo is not None


async def test_market_order_lifecycle(harness: ContractHarness) -> None:
    broker, rec = harness.broker, harness.recorder
    inst = await harness.instrument()
    partial = broker.capabilities.partial_close
    volume = inst.min_volume * 2 if partial else inst.min_volume

    # market buy -> fill
    req = harness.request(Side.BUY, volume)
    ack = await broker.place_order(req)
    assert ack.client_order_id == req.client_order_id
    fill = (await harness.fill_for(req.client_order_id)).fill
    assert fill.volume == volume
    assert fill.side is Side.BUY
    await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == req.client_order_id and e.order.status is OrderStatus.FILLED,
    )
    position_id = fill.position_id
    assert any(p.position_id == position_id for p in await broker.positions())

    # modify SL/TP
    tick = await harness.last_tick()
    sl = inst.round_price(tick.bid * Decimal("0.98"))
    tp = inst.round_price(tick.bid * Decimal("1.02"))
    mark = rec.mark
    await broker.modify_position(position_id, sl, tp)
    await rec.wait_for(
        PositionEvent,
        lambda e: e.position.position_id == position_id and e.change is PositionChange.MODIFIED,
        after=mark,
    )
    pos = next(p for p in await broker.positions() if p.position_id == position_id)
    assert (pos.sl, pos.tp) == (sl, tp)

    # partial close
    if partial:
        mark = rec.mark
        await broker.close_position(position_id, inst.min_volume)
        ev = await rec.wait_for(
            PositionEvent, lambda e: e.change is PositionChange.PARTIALLY_CLOSED, after=mark
        )
        assert ev.position.volume == volume - inst.min_volume

    # full close
    mark = rec.mark
    await broker.close_position(position_id)
    await rec.wait_for(
        PositionEvent,
        lambda e: e.position.position_id == position_id and e.change is PositionChange.CLOSED,
        after=mark,
    )
    assert all(p.position_id != position_id for p in await broker.positions())


async def test_pending_order_lifecycle(harness: ContractHarness) -> None:
    broker, rec = harness.broker, harness.recorder
    inst = await harness.instrument()
    tick = await harness.last_tick()
    far = inst.round_price(tick.bid * Decimal("0.95"))

    req = harness.request(Side.BUY, inst.min_volume, order_type=OrderType.LIMIT, limit_price=far)
    ack = await broker.place_order(req)
    working = await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == req.client_order_id and e.order.status is OrderStatus.WORKING,
    )
    order_id = ack.broker_order_id or working.order.broker_order_id
    assert order_id
    assert any(o.client_order_id == req.client_order_id for o in await broker.open_orders())

    # modify price
    if broker.capabilities.modify_pending:
        new_price = inst.round_price(far - inst.tick_size * 10)
        await broker.modify_order(order_id, OrderChanges(limit_price=new_price))
        await rec.wait_for(
            OrderEvent,
            lambda e: (
                e.order.client_order_id == req.client_order_id and e.order.request.limit_price == new_price
            ),
        )
        listed = next(o for o in await broker.open_orders() if o.client_order_id == req.client_order_id)
        assert listed.request.limit_price == new_price

    # cancel
    await broker.cancel_order(order_id)
    await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == req.client_order_id and e.order.status is OrderStatus.CANCELLED,
    )
    assert all(o.client_order_id != req.client_order_id for o in await broker.open_orders())


async def test_candles_for_each_timeframe(harness: ContractHarness) -> None:
    broker = harness.broker
    tick = await harness.last_tick()
    for tf, depth in broker.capabilities.candle_history.items():
        duration = tf.duration
        assert duration is not None
        assert depth >= duration
        end = tick.ts
        start = end - duration * 10
        bars = await broker.candles(harness.symbol, tf, start, end)
        assert 0 < len(bars) <= min(11, broker.capabilities.max_candles_per_request), tf
        assert all(b.timeframe is tf and b.symbol == harness.symbol for b in bars)
        assert all(start - duration < b.start <= end for b in bars)
        assert [b.start for b in bars] == sorted(b.start for b in bars)
        assert all(b.low <= b.high for b in bars)


async def test_disconnect_reconnect_reconcile(harness: ContractHarness) -> None:
    broker, rec = harness.broker, harness.recorder
    inst = await harness.instrument()
    req = harness.request(Side.SELL, inst.min_volume)
    await broker.place_order(req)
    position_id = (await harness.fill_for(req.client_order_id)).fill.position_id

    mark = rec.mark
    await harness.force_disconnect()
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=mark)
    await harness.reconnect()
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=mark)

    snap = await broker.snapshot()
    assert {p.position_id for p in snap.positions} == {p.position_id for p in await broker.positions()}
    assert position_id in {p.position_id for p in snap.positions}
    assert {o.client_order_id for o in snap.orders} == {o.client_order_id for o in await broker.open_orders()}
    assert snap.account.currency == (await broker.account()).currency

    # Trade events flow again after reconcile.
    mark = rec.mark
    await broker.close_position(position_id)
    await rec.wait_for(
        PositionEvent,
        lambda e: e.position.position_id == position_id and e.change is PositionChange.CLOSED,
        after=mark,
    )


async def test_duplicate_client_order_id_creates_one_order(harness: ContractHarness) -> None:
    broker, rec = harness.broker, harness.recorder
    inst = await harness.instrument()
    req = harness.request(Side.BUY, inst.min_volume)
    first = await broker.place_order(req)
    await harness.fill_for(req.client_order_id)
    second = await broker.place_order(req)  # retry with the same client_order_id
    # Async brokers may not know the order id at ACK time; when both ACKs carry one, they must match.
    if first.broker_order_id is not None and second.broker_order_id is not None:
        assert second.broker_order_id == first.broker_order_id
    await asyncio.sleep(0.3)  # give a (wrongly) duplicated order time to fill

    fills = [e.fill for e in rec.of(FillEvent) if e.fill.client_order_id == req.client_order_id]
    assert len(fills) == 1
    positions = [p for p in await broker.positions() if p.position_id == fills[0].position_id]
    assert [p.volume for p in positions] == [inst.min_volume]


async def test_rate_limit_handling(harness: ContractHarness) -> None:
    """Hammer a cheap endpoint past the advertised burst. Either the adapter throttles client-side
    (every call succeeds) or it raises ``RateLimited`` with a positive retry hint, after which calls work."""
    broker = harness.broker
    limits = await broker.limits()
    assert all(b.rate_per_s > 0 and b.burst > 0 for b in limits.buckets)
    burst = max((b.burst for b in limits.buckets), default=20)

    limited: list[RateLimited] = []
    for _ in range(burst + 10):
        try:
            await broker.account()
        except RateLimited as exc:
            limited.append(exc)
    assert all(exc.retry_after_ms is not None and exc.retry_after_ms > 0 for exc in limited)
    if limited:
        await asyncio.sleep(max(exc.retry_after_ms or 0 for exc in limited) / 1000 + 0.05)
    await broker.account()


async def test_requests_fail_cleanly_while_disconnected(harness: ContractHarness) -> None:
    inst = await harness.instrument()
    await harness.force_disconnect()
    with pytest.raises(BrokerUnavailable):
        await harness.broker.place_order(harness.request(Side.BUY, inst.min_volume))
    await harness.reconnect()
    await harness.broker.snapshot()
    assert await harness.broker.account()


async def test_find_order_by_client_order_id(harness: ContractHarness) -> None:
    """Targeted reconcile: the broker's view of an order we sent, by client_order_id."""
    from datetime import UTC, datetime, timedelta

    broker = harness.broker
    since = datetime.now(UTC) - timedelta(minutes=5)
    inst = await harness.instrument()
    req = harness.request(Side.BUY, inst.min_volume)
    await broker.place_order(req)
    await harness.fill_for(req.client_order_id)
    found = await broker.find_order(req.client_order_id, since)
    assert found is not None
    assert found.client_order_id == req.client_order_id
    assert found.status is OrderStatus.FILLED

    tick = await harness.last_tick()
    pending = harness.request(
        Side.BUY,
        inst.min_volume,
        order_type=OrderType.LIMIT,
        limit_price=inst.round_price(tick.bid * Decimal("0.95")),
    )
    await broker.place_order(pending)
    await harness.recorder.wait_for(
        OrderEvent,
        lambda e: (
            e.order.client_order_id == pending.client_order_id and e.order.status is OrderStatus.WORKING
        ),
    )
    open_view = await broker.find_order(pending.client_order_id, since)
    assert open_view is not None
    assert open_view.status is OrderStatus.WORKING
    assert await broker.find_order("01NEVERSENTNEVERSENTNEVER0", since) is None
    assert isinstance(broker.capabilities.idempotent_submit, bool)
