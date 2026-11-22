"""Restart mid-order (phase 5 acceptance, docs/adr/0007-oms.md).

The process dies after PENDING_SUBMIT was persisted but before it learned the outcome. A fresh process with
the same store must reconcile before trading and end with exactly one position, attributed to the
strategy. It resends only when the broker never saw the order. On Exness, which cannot look orders up by
client id, the resend is safe because the server de-duplicates it by idempotency key.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tests.integration.ctrader_fake import FakeCTraderServer
from tests.integration.exness_fake import FakeExness
from tests.integration.oms_helpers import Node, boot, buy, make_env, settings
from tests.integration.test_ctrader_adapter import make_broker as ctrader_broker
from tests.integration.test_exness_adapter import settings as exness_settings
from trader.brokers.base import Broker
from trader.brokers.exness import ExnessBroker
from trader.domain import OrderStatus
from trader.domain.events import ReconcileEvent
from trader.domain.time import SystemClock
from trader.oms import JsonFileStore, OmsState

D = Decimal


def persisted(path: Path) -> OmsState:
    state = JsonFileStore(path).load()
    assert state is not None
    return state


def only_order(state: OmsState) -> tuple[str, str]:
    [data] = state.orders
    request = data["request"]
    assert isinstance(request, dict)
    return str(request["client_order_id"]), str(data["status"])


def kinds(node: Node) -> list[str]:
    return [e.kind for e in node.rec.of(ReconcileEvent)]


async def eventually(check: Callable[[], bool], timeout: float = 10) -> None:
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.02)


async def crash_mid_submit(node: Node, step: str, path: Path) -> str:
    node.broker.script = [step]
    submit = asyncio.create_task(node.oms.submit(buy("0.10")))
    await node.crash()
    submit.cancel()
    cid, status = only_order(persisted(path))
    assert status == OrderStatus.PENDING_SUBMIT.value  # persisted before the send, never updated
    return cid


# ---------------------------------------------------------------------------------------------- paper


async def test_paper_crash_after_send_finds_the_fill_without_resending(tmp_path: Path) -> None:
    path = tmp_path / "oms.json"
    env = await make_env(store=JsonFileStore(path))
    node = Node(broker=env.broker, oms=env.oms, runtime=env.runtime, rec=env.rec)
    cid = await crash_mid_submit(node, "crash_after", path)
    await env.paper.simulate_disconnect("process died")
    env.paper.discard_events()
    await env.quote("1.10020")  # the venue keeps running while we are down

    restarted = await boot(env.paper, JsonFileStore(path))
    try:
        order = restarted.oms.order(cid)
        assert order is not None
        assert order.status is OrderStatus.FILLED
        assert order.filled_volume == D("0.10")
        assert restarted.broker.sent == []  # found, not resent
        [pos] = restarted.oms.positions()
        assert (pos.strategy_id, pos.volume) == ("ema", D("0.10"))
        assert len(await env.paper.positions()) == 1
        assert "order_resolved" in kinds(restarted)
        # Attribution is persisted, so a later restart on an adapter that lost it still knows the owner.
        await eventually(lambda: persisted(path).position_strategy == {pos.position_id: "ema"})
    finally:
        await restarted.close()


async def test_paper_crash_before_send_resubmits_once(tmp_path: Path) -> None:
    path = tmp_path / "oms.json"
    env = await make_env(store=JsonFileStore(path))
    node = Node(broker=env.broker, oms=env.oms, runtime=env.runtime, rec=env.rec)
    cid = await crash_mid_submit(node, "crash_before", path)
    assert env.broker.sent == []
    await env.paper.simulate_disconnect("process died")
    env.paper.discard_events()

    restarted = await boot(env.paper, JsonFileStore(path))
    try:
        await eventually(
            lambda: (o := restarted.oms.order(cid)) is not None and o.status is OrderStatus.FILLED
        )
        assert [r.client_order_id for r in restarted.broker.sent] == [cid]  # same id, sent once
        assert "order_resubmitted" in kinds(restarted)
        [pos] = await env.paper.positions()
        assert pos.strategy_id == "ema"
        await eventually(lambda: [p.position_id for p in restarted.oms.positions()] == [pos.position_id])
    finally:
        await restarted.close()


async def test_paper_long_outage_abandons_instead_of_sending_a_stale_signal(tmp_path: Path) -> None:
    path = tmp_path / "oms.json"
    env = await make_env(store=JsonFileStore(path))
    node = Node(broker=env.broker, oms=env.oms, runtime=env.runtime, rec=env.rec)
    cid = await crash_mid_submit(node, "crash_before", path)
    await env.paper.simulate_disconnect("process died")
    env.paper.discard_events()

    restarted = await boot(env.paper, JsonFileStore(path), settings(resubmit_window=timedelta(0)))
    try:
        order = restarted.oms.order(cid)
        assert order is not None
        assert order.status is OrderStatus.REJECTED
        assert restarted.broker.sent == []
        assert await env.paper.positions() == []
        assert "order_abandoned" in kinds(restarted)
    finally:
        await restarted.close()


# ------------------------------------------------------------------------------------ real adapters


@pytest.fixture
async def ctrader_server() -> AsyncIterator[FakeCTraderServer]:
    server = FakeCTraderServer()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def exness_server() -> AsyncIterator[FakeExness]:
    server = FakeExness()
    await server.start()
    yield server
    await server.stop()


async def restart_scenario(
    tmp_path: Path, new_broker: Callable[[], Broker], executed: Callable[[], bool]
) -> tuple[Node, str]:
    """Crash after the broker received the order, wait until the venue has executed it, close the dead
    process's connection, then start a new process with a new adapter instance and the same store.
    Returns the restarted node and the order id."""
    path = tmp_path / "oms.json"
    first = await boot(new_broker(), JsonFileStore(path))
    cid = await crash_mid_submit(first, "crash_after", path)
    await eventually(executed)
    await first.broker.inner.close()  # the OS closes a dead process's sockets
    return await boot(new_broker(), JsonFileStore(path)), cid


async def assert_one_attributed_position(node: Node, cid: str, broker_positions: Awaitable[int]) -> None:
    await eventually(lambda: (o := node.oms.order(cid)) is not None and o.status is OrderStatus.FILLED)
    await eventually(lambda: [p.strategy_id for p in node.oms.positions()] == ["ema"])
    assert await broker_positions == 1


async def test_ctrader_restart_finds_the_order_in_history(
    tmp_path: Path, ctrader_server: FakeCTraderServer
) -> None:
    node, cid = await restart_scenario(
        tmp_path, lambda: ctrader_broker(ctrader_server), lambda: len(ctrader_server.positions) == 1
    )
    try:
        await assert_one_attributed_position(node, cid, _count(node))
        assert node.broker.sent == []
        assert "order_resolved" in kinds(node)
    finally:
        await node.close()


async def test_exness_restart_resends_idempotently_and_recovers_the_link(
    tmp_path: Path, exness_server: FakeExness
) -> None:
    def new_broker() -> Broker:
        return ExnessBroker(
            exness_settings(exness_server), clock=SystemClock(), http_transport=exness_server.transport
        )

    node, cid = await restart_scenario(tmp_path, new_broker, lambda: len(exness_server.positions) == 1)
    try:
        await assert_one_attributed_position(node, cid, _count(node))
        # The new adapter could not find the order by client id, so the OMS resent it with the same key.
        # The server returned the original operation without executing it again. No transaction event
        # followed, so the adapter's watchdog re-linked the order from deal/order history, and the
        # reconcile after its resubscribe resolved the order and re-attributed the position.
        assert [r.client_order_id for r in node.broker.sent] == [cid]
        assert len(exness_server.positions) == 1
        assert kinds(node)[:2] == ["position_adopted", "order_resubmitted"]
        assert {"position_corrected", "order_resolved"} <= set(kinds(node))
    finally:
        await node.close()


async def _count(node: Node) -> int:
    return len(await node.broker.positions())
