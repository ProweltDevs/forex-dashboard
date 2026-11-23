"""Phase 11 acceptance: the chaos suite. A real ``trader run`` process against a broker that outlives it
(tests/chaos/harness.py). After every scenario the broker and the trader must agree: each order executed
exactly once, none lost, none left in flight, positions equal.

Also part of the suite (marker ``chaos``): ``tests/integration/test_db_kill.py`` (PostgreSQL killed
mid-run) and the notification-outage test below.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path

import httpx
import pytest

from tests.chaos.harness import ACCOUNT, Trader, executed, order_body
from tests.integration.api_helpers import api_config, running_app, wait_for
from tests.integration.ctrader_fake import FakeCTraderServer
from trader.notify.notifier import ChannelError, Severity

pytestmark = pytest.mark.chaos

TERMINAL = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}


async def manual_orders(t: Trader) -> dict[str, dict[str, object]]:
    async with t.api() as api:
        orders = (await api.get("/orders", params={"strategy": "manual"})).json()
    return {o["client_order_id"]: o for o in orders}


async def wait_resolved(t: Trader, cids: set[str], timeout: float = 45.0) -> dict[str, dict[str, object]]:
    async with asyncio.timeout(timeout):
        while True:
            orders = await manual_orders(t)
            if cids <= set(orders) and all(orders[c]["status"] in TERMINAL for c in cids):
                return orders
            await asyncio.sleep(0.2)


async def positions_agree(t: Trader, broker: FakeCTraderServer) -> None:
    async with t.api() as api:
        ours = {p["position_id"] for p in (await api.get("/positions")).json()}
    assert ours == {str(pid) for pid in broker.positions}


async def test_sigterm_with_an_order_in_flight_is_graceful(trader: Trader, broker: FakeCTraderServer) -> None:
    await trader.start()
    await trader.wait_health()
    broker.ack_delay_s = 1.5  # the order is still PENDING_SUBMIT when SIGTERM arrives
    async with trader.api() as api:
        submit = asyncio.create_task(api.post("/orders", json=order_body()))
        await wait_for(lambda: len(broker.new_order_requests) == 1, what="the request reached the broker")
        code = await trader.terminate()
        resp = await submit  # answered before the API went away: shutdown let the order settle
    assert code == 0, trader.log()[-2000:]
    order = resp.json()
    assert order["status"] in ("ACCEPTED", "FILLED")
    assert trader.events("app_stopped")
    assert not trader.events("shutdown_orders_still_in_flight")
    cid = order["client_order_id"]
    assert trader.oms_orders()[cid] == "FILLED"  # persisted in its final state, not PENDING_SUBMIT
    assert executed(broker) == {cid: 1}
    reports = list((trader.data_dir / "reports").glob("run-*.json"))
    assert len(reports) == 1
    assert json.loads(reports[0].read_text())["strategy_errors"] == 0


async def test_sigkill_mid_order_then_restart_resolves_every_order(
    trader: Trader, broker: FakeCTraderServer
) -> None:
    await trader.start()
    await trader.wait_health()
    broker.ack_delay_s = 1.0  # executions happen after the process is dead
    broker.swallow_orders = 1  # ... and the first request is lost on the way: the broker never saw it
    async with trader.api() as api:
        sends = [
            asyncio.create_task(api.post("/orders", json=order_body(side))) for side in ("BUY", "SELL", "BUY")
        ]
        await wait_for(lambda: len(broker.new_order_requests) == 3, what="three requests left the process")
        in_flight = trader.oms_orders()
        await trader.kill()  # SIGKILL: no shutdown code at all
        for task in sends:
            with contextlib.suppress(httpx.HTTPError):
                await task
    cids = set(broker.new_order_requests)
    assert len(cids) == 3
    assert {in_flight[c] for c in cids} == {"PENDING_SUBMIT"}  # persisted before they were sent
    lost = broker.new_order_requests[0]
    await asyncio.sleep(1.5)  # the broker executes the two it received, while nobody is connected
    assert executed(broker) == dict.fromkeys(cids - {lost}, 1)

    broker.ack_delay_s = 0
    await trader.start()  # same data directory: restore, reconnect, reconcile
    await trader.wait_health()
    orders = await wait_resolved(trader, cids)
    assert {orders[c]["status"] for c in cids} == {"FILLED"}
    assert executed(broker) == dict.fromkeys(cids, 1)  # every order exactly once: nothing lost, no duplicate
    assert broker.new_order_requests.count(lost) == 2  # resent with the same client_order_id
    assert all(broker.new_order_requests.count(c) == 1 for c in cids - {lost})  # the others never resent
    await positions_agree(trader, broker)
    assert await trader.terminate() == 0


async def test_connection_dropped_with_an_order_in_flight(trader: Trader, broker: FakeCTraderServer) -> None:
    await trader.start()
    await trader.wait_health()
    async with trader.api() as api:
        first = (await api.post("/orders", json=order_body())).json()
        assert first["status"] in ("ACCEPTED", "FILLED")
        broker.ack_delay_s = 1.0
        send = asyncio.create_task(api.post("/orders", json=order_body("SELL")))
        await wait_for(lambda: len(broker.new_order_requests) == 2, what="second request sent")
        await broker.drop_all()  # TCP reset under the trader; the order executes while it reconnects
        with contextlib.suppress(httpx.HTTPError):
            await send
        broker.ack_delay_s = 0
        await trader.wait_health()
        cids = set(broker.new_order_requests)
        orders = await wait_resolved(trader, cids)
        assert {orders[c]["status"] for c in cids} == {"FILLED"}
        assert executed(broker) == dict.fromkeys(cids, 1)
        assert len(broker.new_order_requests) == 2  # the in-flight order was found, not resent
        third = (await api.post("/orders", json=order_body())).json()  # trading resumed
        assert third["status"] in ("ACCEPTED", "FILLED")
    await positions_agree(trader, broker)
    text = await trader.metrics()
    assert f'trader_broker_disconnects_total{{account="{ACCOUNT}"}} 1.0' in text
    assert f'trader_broker_reconnects_total{{account="{ACCOUNT}"}} 1.0' in text
    assert await trader.terminate() == 0


async def test_broker_unreachable_then_back(trader: Trader, broker: FakeCTraderServer) -> None:
    await trader.start()
    await trader.wait_health()
    await broker.go_down()  # connections dropped and refused
    body = await trader.wait_health(503)
    assert body["status"] == "not_ready"
    assert body["not_ready"] == [ACCOUNT]
    async with trader.api() as api:
        refused = await api.post("/orders", json=order_body())
        assert refused.status_code == 503  # refused, not queued: nothing fires when the broker returns
        assert "not reconciled" in refused.json()["detail"]
        await asyncio.sleep(2)  # several failed reconnect attempts
        await broker.come_back()
        await trader.wait_health()
        order = (await api.post("/orders", json=order_body())).json()
        assert order["status"] in ("ACCEPTED", "FILLED")
    assert len(broker.new_order_requests) == 1  # the refused order never reached the broker
    await wait_resolved(trader, set(broker.new_order_requests))
    await positions_agree(trader, broker)
    assert await trader.terminate() == 0


async def test_broker_down_at_startup_exits_and_a_supervisor_restart_recovers(
    trader: Trader, broker: FakeCTraderServer
) -> None:
    """With a broker unreachable at startup the process exits non-zero (after stopping what it started) and
    the supervisor (Docker ``restart: unless-stopped``, systemd) starts it again; once the broker is back,
    a restart recovers on its own: reconcile first, then trade."""
    await trader.start()
    await trader.wait_health()
    async with trader.api() as api:
        filled = (await api.post("/orders", json=order_body())).json()
    await trader.kill()
    await broker.go_down()
    await trader.start()
    assert trader.proc is not None
    async with asyncio.timeout(30):
        code = await trader.proc.wait()
    assert code == 2
    assert "run failed: cannot connect" in trader.log()
    assert trader.events("app_stopped")  # what had started was stopped cleanly
    await broker.come_back()
    await trader.start()  # what the supervisor does
    await trader.wait_health()
    orders = await manual_orders(trader)
    assert orders[filled["client_order_id"]]["status"] == "FILLED"
    await positions_agree(trader, broker)
    assert executed(broker) == {filled["client_order_id"]: 1}
    assert await trader.terminate() == 0


async def test_trader_kill_is_refused_while_trader_run_manages_the_account(
    trader: Trader, broker: FakeCTraderServer
) -> None:
    """Two writers of one account's OMS state would undo each other (the CLI's kill switch overwritten by
    the running process). The CLI is refused and says what to use instead; the running process is
    untouched."""
    await trader.start()
    await trader.wait_health()
    code, out = await trader.cli("kill", "--account", ACCOUNT, "--level", "FLATTEN_ALL", "--reason", "chaos")
    assert code == 2, out
    assert "another process" in out
    assert "POST /risk/kill" in out
    body = await trader.wait_health()
    assert body["accounts"][ACCOUNT]["kill_level"] == "NONE"
    assert broker.new_order_requests == []  # the refused CLI never reached the broker
    assert await trader.terminate() == 0
    code, out = await trader.cli("kill", "--account", ACCOUNT, "--level", "PAUSE_NEW", "--reason", "chaos")
    assert code == 0, out  # with the process stopped, the CLI is the way
    await trader.start()
    body = await trader.wait_health()
    assert body["accounts"][ACCOUNT]["kill_level"] == "PAUSE_NEW"  # restored from the CLI's snapshot
    assert await trader.terminate() == 0


class DeadChannel:
    """Telegram (or SMTP) down: every send fails."""

    name = "dead"
    min_severity = Severity.INFO

    def __init__(self) -> None:
        self.attempts = 0

    async def send(self, subject: str, text: str) -> None:
        self.attempts += 1
        raise ChannelError("unreachable")

    async def close(self) -> None:
        return None


async def test_notifications_down_never_affect_trading(tmp_path: Path) -> None:
    dead = DeadChannel()
    async with running_app(api_config(tmp_path), channels=[dead]) as run:
        oms = run.app.accounts["paper-main"].oms
        await wait_for(lambda: len([o for o in oms.orders() if o.status.value == "FILLED"]) >= 2, 30, "fills")
        assert run.app.runner is not None
        assert all(stats.total_errors == 0 for _sid, _st, _r, stats in run.app.runner.status())
        await wait_for(lambda: dead.attempts >= 3, 20, "sends were attempted")
        assert (await run.client.get("/health")).json()["status"] == "ok"
    assert run.app.notifications is not None
    assert run.app.notifications.notifier.stats.sent == 0
