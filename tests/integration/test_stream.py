"""``/stream`` over a real WebSocket: auth (header or first message, never the URL), subscribe, throttled
ticks, order/fill events as they happen, input validation, and lag reporting for a slow client."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from tests.integration.api_helpers import ACCOUNT, TOKEN, Running, api_config, running_app, wait_for
from trader.api.stream import _Client, _flush
from trader.app import App
from trader.domain.events import AlertEvent
from trader.domain.time import SystemClock
from trader.marketdata.bus import Bus, Overflow


@pytest.fixture
async def run(tmp_path: Path) -> AsyncIterator[Running]:
    async with running_app(api_config(tmp_path)) as r:
        await wait_for(lambda: r.app.accounts[ACCOUNT].oms.view().last_ticks, what="first tick")
        yield r


def ws_url(run: Running) -> str:
    return run.base.replace("http://", "ws://") + "/stream"


async def recv(ws: ClientConnection, timeout: float = 5.0) -> dict[str, Any]:
    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout))
    assert isinstance(msg, dict)
    return msg


async def recv_until(ws: ClientConnection, pred: Any, timeout: float = 10.0) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        while True:
            msg = await recv(ws, timeout)
            if pred(msg):
                return msg


async def test_bad_or_missing_token_closes_with_1008(run: Running) -> None:
    for headers in ({}, {"Authorization": "Bearer nope"}):
        async with connect(ws_url(run), additional_headers=headers) as ws:
            if not headers:
                await ws.send(json.dumps({"op": "auth", "token": "wrong"}))
            with pytest.raises(ConnectionClosed) as info:
                await recv(ws)
            assert info.value.rcvd is not None
            assert info.value.rcvd.code == 1008


async def test_auth_by_first_message_then_events_and_ticks(run: Running) -> None:
    async with connect(ws_url(run)) as ws:  # a browser: no headers, the token in the first message
        await ws.send(json.dumps({"op": "auth", "token": TOKEN}))
        hello = await recv(ws)
        assert hello["type"] == "hello"
        assert "ticks" in hello["topics"]
        assert "orders" in hello["topics"]
        await ws.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "topics": ["ticks", "fills"],
                    "symbols": ["EURUSD"],
                    "tick_interval_ms": 100,
                }
            )
        )
        sub = await recv_until(ws, lambda m: m["type"] == "subscribed")
        assert sub == {
            "type": "subscribed",
            "topics": ["fills", "ticks"],
            "symbols": ["EURUSD"],
            "tick_interval_ms": 100,
        }
        ticks = await recv_until(ws, lambda m: m["type"] == "ticks")
        [t] = ticks["data"]  # coalesced: the latest tick per (feed, symbol) only
        assert t["symbol"] == "EURUSD"
        assert Decimal(t["ask"]) >= Decimal(t["bid"])
        datetime.fromisoformat(t["ts"])  # ISO-8601 like the REST API

        # Batches are throttled: ~10 per second at 100 ms whatever the feed rate (50 ticks/s here).
        start = asyncio.get_running_loop().time()
        batches = 0
        while asyncio.get_running_loop().time() - start < 1.0:
            m = await recv(ws)
            batches += m["type"] == "ticks"
        assert 5 <= batches <= 12


async def test_order_and_fill_events_arrive_as_they_happen(run: Running) -> None:
    async with connect(ws_url(run), additional_headers={"Authorization": f"Bearer {TOKEN}"}) as ws:
        assert (await recv(ws))["type"] == "hello"
        await ws.send(json.dumps({"op": "subscribe", "topics": ["orders", "fills", "positions", "risk"]}))
        await recv_until(ws, lambda m: m["type"] == "subscribed")
        bid, _ = run.price()
        body = {
            "account": ACCOUNT,
            "symbol": "EURUSD",
            "side": "BUY",
            "volume": "0.01",
            "sl": str(bid - Decimal("0.005")),
        }
        order = (await run.client.post("/orders", json=body)).json()
        cid = order["client_order_id"]
        seen: dict[str, dict[str, Any]] = {}

        def mine(m: dict[str, Any]) -> bool:
            if m.get("kind") == "FillEvent" and m["data"]["fill"]["client_order_id"] == cid:
                seen["fill"] = m
            if (
                m.get("kind") == "OrderEvent"
                and m["data"]["order"]["request"]["client_order_id"] == cid
                and m["data"]["order"]["status"] == "FILLED"
            ):
                seen["filled"] = m
            return len(seen) == 2

        await recv_until(ws, mine)
        filled, fill = seen["filled"], seen["fill"]
        assert filled["topic"] == "orders"
        assert filled["account_id"] == ACCOUNT
        assert filled["data"]["order"]["request"]["strategy_id"] == "manual"
        assert fill["data"]["fill"]["volume"] == "0.01"
        await run.client.post(
            "/risk/kill", json={"account": ACCOUNT, "level": "PAUSE_NEW", "reason": "stream"}
        )
        risk = await recv_until(ws, lambda m: m.get("kind") == "RiskStateEvent")
        assert risk["data"]["level"] == "PAUSE_NEW"


async def drain(ws: ClientConnection) -> None:
    while True:
        await recv(ws)


async def test_bad_subscribe_is_refused_and_the_stream_ends_cleanly_at_shutdown(tmp_path: Path) -> None:
    app = App(api_config(tmp_path), env={"TRADER_API_TOKEN": TOKEN}, clock=SystemClock())
    await app.start()
    try:
        assert app.api is not None
        url = f"ws://127.0.0.1:{app.api.port}/stream"
        async with connect(url, additional_headers={"Authorization": f"Bearer {TOKEN}"}) as ws:
            await recv(ws)
            for bad in ({"op": "subscribe", "topics": ["nope"]}, {"op": "what"}, ["x"]):
                await ws.send(json.dumps(bad))
                assert (await recv_until(ws, lambda m: m["type"] != "event"))["type"] == "error"
            await ws.send("not json")  # refused, and the stream carries on
            assert (await recv_until(ws, lambda m: m["type"] != "event"))["type"] == "error"
            await ws.send(json.dumps({"op": "subscribe", "topics": ["alerts"]}))
            assert (await recv_until(ws, lambda m: m["type"] != "event"))["type"] == "subscribed"
            report = await app.stop()  # the server goes away under a connected client
            with pytest.raises(ConnectionClosed):
                await drain(ws)
    except BaseException:
        await app.stop()
        raise
    assert report.strategy_errors == 0


class _Recorder:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_json(self, data: dict[str, Any]) -> None:
        self.sent.append(data)

    async def send_text(self, text: str) -> None:
        self.sent.append(json.loads(text))


async def test_a_slow_client_loses_the_oldest_events_and_is_told() -> None:
    bus: Bus[Any] = Bus()
    events = bus.subscribe("client", policy=Overflow.DROP_OLDEST, maxsize=3)
    now = datetime.now(UTC)
    for i in range(10):
        bus.publish(AlertEvent(account_id=ACCOUNT, kind=f"k{i}", severity="info", message="m", ts=now))
    ws = _Recorder()
    client = _Client()
    await _flush(ws, client, events, None)  # type: ignore[arg-type]
    assert ws.sent == [{"type": "lagged", "dropped": 7}]
    kinds = []
    while (e := events.get_nowait()) is not None:
        kinds.append(e.kind)
    assert kinds == ["k7", "k8", "k9"]  # the newest survive
    await _flush(ws, client, events, None)  # type: ignore[arg-type]
    assert len(ws.sent) == 1  # reported once
