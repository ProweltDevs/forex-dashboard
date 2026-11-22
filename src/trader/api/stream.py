"""``/stream``: the WebSocket push channel for the UI (SPEC §13).

Protocol (JSON text frames):
- Auth: an ``Authorization: Bearer <token>`` header on the upgrade, or, for browsers that cannot set
  headers, ``{"op": "auth", "token": "..."}`` as the first message within 5 s. Never in the URL (URLs end up
  in proxy logs). A bad or missing token closes the socket with code 1008.
- The server sends ``{"type": "hello", "topics": [...]}``. The client then sends
  ``{"op": "subscribe", "topics": [...], "symbols": [...], "tick_interval_ms": 500}`` (any time, to change
  it). Default: every event topic, no ticks.
- Events: ``{"type": "event", "topic": "orders", "kind": "OrderEvent", "account_id": ..., "data": {...}}``.
- Ticks and account updates are throttled per client: at most one batch per ``tick_interval_ms``, holding
  only the latest tick per (feed, symbol) and the latest state per account:
  ``{"type": "ticks", "data": [...]}``, ``{"type": "event", "topic": "account", ...}``.

Backpressure: every client has a bounded queue (``DROP_OLDEST``). A client too slow to keep up loses the
oldest events and is told so: ``{"type": "lagged", "dropped": n}``. Trading never waits on a client.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from trader.domain.events import (
    AccountEvent,
    AlertEvent,
    BarEvent,
    ConnectionEvent,
    DomainEvent,
    FeedEvent,
    FillEvent,
    OrderEvent,
    OrderRetryEvent,
    PositionEvent,
    ReconcileEvent,
    RiskEvent,
    RiskStateEvent,
    StrategyEvent,
    TickEvent,
)
from trader.domain.jsontypes import JsonObj
from trader.marketdata.bus import Bus, Overflow, Subscription, SubscriptionClosed
from trader.observability.logging import get_logger
from trader.persistence.serialize import to_json

_log = get_logger("trader.api.stream")

TOPICS: dict[type, str] = {
    OrderEvent: "orders",
    OrderRetryEvent: "orders",
    FillEvent: "fills",
    PositionEvent: "positions",
    AccountEvent: "account",
    ConnectionEvent: "connection",
    FeedEvent: "feed",
    ReconcileEvent: "reconcile",
    RiskEvent: "risk",
    RiskStateEvent: "risk",
    StrategyEvent: "strategies",
    AlertEvent: "alerts",
}
EVENT_TOPICS = tuple(sorted(set(TOPICS.values())))
ALL_TOPICS = (*EVENT_TOPICS, "ticks")

AUTH_TIMEOUT_S = 5.0
CLIENT_QUEUE = 2_000
MAX_CLIENTS = 32
MIN_INTERVAL_MS = 50


def event_message(event: object) -> JsonObj:
    data = to_json(event, iso_times=True)
    account = getattr(event, "account_id", None)
    return {
        "type": "event",
        "topic": TOPICS.get(type(event), "other"),
        "kind": type(event).__name__,
        "account_id": account if isinstance(account, str) else None,
        "data": data,
    }


def tick_json(event: TickEvent) -> JsonObj:
    t = event.tick
    return {
        "feed": event.broker,
        "symbol": t.symbol,
        "bid": str(t.bid),
        "ask": str(t.ask),
        "ts": t.ts.isoformat(),
    }


class StreamHub:
    """Fans every event bus of the app into one bus the clients subscribe to."""

    def __init__(
        self, sources: Iterable[Bus[DomainEvent] | Bus[StrategyEvent]], ticks: Bus[TickEvent]
    ) -> None:
        self._sources = list(sources)
        self.ticks = ticks
        self.events: Bus[DomainEvent] = Bus()
        self._tasks: list[asyncio.Task[None]] = []
        self.clients = 0

    async def start(self) -> None:
        for i, bus in enumerate(self._sources):
            sub = bus.subscribe(f"stream-hub-{i}")
            self._tasks.append(asyncio.create_task(self._forward(sub), name=f"stream-hub-{i}"))

    async def _forward(self, sub: Subscription[Any]) -> None:
        async for event in sub:
            if not isinstance(event, TickEvent | BarEvent):
                self.events.publish(event)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.events.close()


@dataclass(slots=True)
class _Client:
    topics: set[str] = field(default_factory=lambda: set(EVENT_TOPICS))
    symbols: set[str] = field(default_factory=set)
    interval_s: float = 0.5
    accounts: dict[str, AccountEvent] = field(default_factory=dict)
    reported_drops: int = 0


def _bearer(ws: WebSocket) -> str | None:
    header = ws.headers.get("authorization", "")
    scheme, _, value = header.partition(" ")
    return value.strip() if scheme.lower() == "bearer" and value.strip() else None


def _token_ok(given: str | None, token: str) -> bool:
    return given is not None and hmac.compare_digest(given.encode(), token.encode())


async def serve_stream(ws: WebSocket, hub: StreamHub, token: str, default_interval_ms: int) -> None:
    await ws.accept()
    if "authorization" in ws.headers:  # a header that is there must be right: no second chance
        if not _token_ok(_bearer(ws), token):
            _log.warning("stream_unauthorized")
            await _close(ws, 1008, "unauthorized")
            return
    else:
        try:
            async with asyncio.timeout(AUTH_TIMEOUT_S):
                first = json.loads(await ws.receive_text())
        except (TimeoutError, ValueError, KeyError, WebSocketDisconnect):
            first = None
        given = first.get("token") if isinstance(first, dict) and first.get("op") == "auth" else None
        if not _token_ok(given if isinstance(given, str) else None, token):
            _log.warning("stream_unauthorized")
            await _close(ws, 1008, "unauthorized")
            return
    if hub.clients >= MAX_CLIENTS:
        await _close(ws, 1013, "too many clients")
        return
    hub.clients += 1
    client = _Client(interval_s=max(MIN_INTERVAL_MS, default_interval_ms) / 1000)
    events = hub.events.subscribe("stream-client", policy=Overflow.DROP_OLDEST, maxsize=CLIENT_QUEUE)
    ticks: Subscription[TickEvent] | None = None
    tick_symbols: frozenset[str] = frozenset()
    reader: asyncio.Task[None] | None = None
    shutdown = False
    try:
        await ws.send_json({"type": "hello", "topics": list(ALL_TOPICS)})
        reader = asyncio.create_task(_read(ws, client))
        loop = asyncio.get_running_loop()
        next_flush = loop.time() + client.interval_s
        while not reader.done():
            try:
                async with asyncio.timeout(max(0.0, next_flush - loop.time())):
                    event = await events.get()  # order, fill, risk... events go out at once
                await _send_event(ws, client, event)
                continue
            except TimeoutError:
                pass
            except SubscriptionClosed:
                shutdown = True
                break
            wanted = frozenset(client.symbols) if "ticks" in client.topics else frozenset()
            if wanted != tick_symbols:
                if ticks is not None:
                    hub.ticks.unsubscribe(ticks)
                ticks = _tick_sub(hub, wanted) if wanted else None
                tick_symbols = wanted
            await _flush(ws, client, events, ticks)
            next_flush = loop.time() + client.interval_s
    except WebSocketDisconnect:
        pass
    finally:
        if reader is not None:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect, Exception):
                await reader
        hub.events.unsubscribe(events)
        if ticks is not None:
            hub.ticks.unsubscribe(ticks)
        hub.clients -= 1
        await _close(ws, 1001 if shutdown else 1000, "server shutting down" if shutdown else "bye")


def _tick_sub(hub: StreamHub, symbols: frozenset[str]) -> Subscription[TickEvent]:
    return hub.ticks.subscribe(
        "stream-ticks",
        policy=Overflow.COALESCE,
        key=lambda e: (e.broker, e.tick.symbol),
        accept=lambda e: e.tick.symbol in symbols,
    )


async def _send_event(ws: WebSocket, client: _Client, event: DomainEvent) -> None:
    topic = TOPICS.get(type(event))
    if topic is None or topic not in client.topics:
        return
    if isinstance(event, AccountEvent):
        client.accounts[event.account_id] = event  # throttled: only the latest per account
        return
    await ws.send_text(json.dumps(event_message(event), separators=(",", ":")))


async def _flush(
    ws: WebSocket, client: _Client, events: Subscription[DomainEvent], ticks: Subscription[TickEvent] | None
) -> None:
    if events.dropped > client.reported_drops:
        await ws.send_json({"type": "lagged", "dropped": events.dropped - client.reported_drops})
        client.reported_drops = events.dropped
    for state in client.accounts.values():
        await ws.send_text(json.dumps(event_message(state), separators=(",", ":")))
    client.accounts.clear()
    if ticks is not None:
        batch = []
        while (tick := ticks.get_nowait()) is not None:
            batch.append(tick_json(tick))
        if batch:
            await ws.send_json({"type": "ticks", "data": batch})


async def _read(ws: WebSocket, client: _Client) -> None:
    """Apply ``subscribe`` messages until the client disconnects."""
    while True:
        try:
            msg = json.loads(await ws.receive_text())
        except ValueError:
            msg = None
        if not isinstance(msg, dict) or msg.get("op") != "subscribe":
            await ws.send_json({"type": "error", "message": 'expected {"op": "subscribe", ...}'})
            continue
        topics = msg.get("topics", list(EVENT_TOPICS))
        symbols = msg.get("symbols", [])
        interval = msg.get("tick_interval_ms", int(client.interval_s * 1000))
        if (
            not isinstance(topics, list)
            or not all(isinstance(t, str) and t in ALL_TOPICS for t in topics)
            or not isinstance(symbols, list)
            or not all(isinstance(x, str) for x in symbols)
            or not isinstance(interval, int)
            or isinstance(interval, bool)
        ):
            await ws.send_json({"type": "error", "message": f"bad subscribe; topics are {list(ALL_TOPICS)}"})
            continue
        client.topics = set(topics)
        client.symbols = set(symbols)
        client.interval_s = max(MIN_INTERVAL_MS, interval) / 1000
        await ws.send_json(
            {
                "type": "subscribed",
                "topics": sorted(client.topics),
                "symbols": sorted(client.symbols),
                "tick_interval_ms": int(client.interval_s * 1000),
            }
        )


async def _close(ws: WebSocket, code: int, reason: str) -> None:
    if ws.client_state is WebSocketState.CONNECTED and ws.application_state is WebSocketState.CONNECTED:
        with contextlib.suppress(Exception):
            await ws.close(code, reason)
