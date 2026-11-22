"""Test plumbing for the OMS: a broker wrapper that injects submit failures / hides capabilities, and an
environment wiring PaperBroker + OrderManager + AccountRuntime."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TypeVar

from tests.factories import eurusd
from trader.brokers.base import Broker, BrokerAck, Capabilities, OrderChanges, RateLimits
from trader.brokers.paper import FakeTickSource, PaperBroker, PaperSettings
from trader.domain import Bar, Instrument, Order, OrderIntent, OrderType, Position, Side, Timeframe
from trader.domain.errors import InvalidVolume, Requote, UnknownOrderState
from trader.domain.events import DomainEvent
from trader.domain.models import AccountState, BrokerSnapshot, Fill, OrderRequest
from trader.domain.time import SystemClock
from trader.oms import AccountRuntime, MemoryStore, OmsSettings, OmsStore, OrderManager
from trader.risk.gate import AccountView, ApproveAll, RiskGate, RiskVerdict

E = TypeVar("E")
D = Decimal


class ScriptedBroker:
    """Delegates to a real broker. ``script`` drives ``place_order``: each call pops one step:
    ok | requote | invalid | unknown_before (never reaches the broker) | unknown_after (reaches it, then
    the outcome is lost) | hang_after (reaches it, never ACKs) | crash_before / crash_after (set
    ``crashed`` and hang before / after the request reaches the broker: the test then kills the OMS)."""

    def __init__(self, inner: Broker, **caps: object) -> None:
        self.inner = inner
        self.caps_override = caps
        self.script: list[str] = []
        self.sent: list[OrderRequest] = []
        self.crashed = asyncio.Event()
        self.lookups: list[str] = []  # find_order calls (client_order_id)

    @property
    def name(self) -> str:
        return self.inner.name

    @property
    def capabilities(self) -> Capabilities:
        return replace(self.inner.capabilities, **self.caps_override)  # type: ignore[arg-type]

    async def place_order(self, req: OrderRequest) -> BrokerAck:
        step = self.script.pop(0) if self.script else "ok"
        if step == "requote":
            raise Requote("price moved")
        if step == "invalid":
            raise InvalidVolume("bad volume")
        if step == "unknown_before":
            raise UnknownOrderState("connection lost before the request left")
        if step == "crash_before":
            self.crashed.set()
            await asyncio.sleep(3600)
        self.sent.append(req)
        ack = await self.inner.place_order(req)
        if step == "crash_after":
            self.crashed.set()
            await asyncio.sleep(3600)
        if step == "unknown_after":
            raise UnknownOrderState("connection lost after the request left")
        if step == "hang_after":
            await asyncio.sleep(3600)
        return ack

    async def connect(self) -> None:
        await self.inner.connect()

    async def close(self) -> None:
        await self.inner.close()

    async def instruments(self) -> list[Instrument]:
        return await self.inner.instruments()

    async def account(self) -> AccountState:
        return await self.inner.account()

    async def positions(self) -> list[Position]:
        return await self.inner.positions()

    async def open_orders(self) -> list[Order]:
        return await self.inner.open_orders()

    async def snapshot(self) -> BrokerSnapshot:
        return await self.inner.snapshot()

    async def deal_history(self, start: datetime, end: datetime) -> list[Fill]:
        return await self.inner.deal_history(start, end)

    async def find_order(self, client_order_id: str, since: datetime) -> Order | None:
        self.lookups.append(client_order_id)
        return await self.inner.find_order(client_order_id, since)

    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck:
        return await self.inner.modify_order(order_id, changes)

    async def cancel_order(self, order_id: str) -> BrokerAck:
        return await self.inner.cancel_order(order_id)

    async def modify_position(self, position_id: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck:
        return await self.inner.modify_position(position_id, sl, tp)

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        return await self.inner.close_position(position_id, volume)

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        return await self.inner.candles(symbol, tf, start, end)

    async def subscribe_ticks(self, symbols: Sequence[str]) -> None:
        await self.inner.subscribe_ticks(symbols)

    def events(self) -> AsyncIterator[DomainEvent]:
        return self.inner.events()

    async def limits(self) -> RateLimits:
        return await self.inner.limits()


class Recorder:
    def __init__(self, oms: OrderManager) -> None:
        self.events: list[DomainEvent] = []
        self._sub = oms.events.subscribe("test-recorder")
        self._changed = asyncio.Condition()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        async for ev in self._sub:
            async with self._changed:
                self.events.append(ev)
                self._changed.notify_all()

    def of(self, kind: type[E]) -> list[E]:
        return [e for e in self.events if isinstance(e, kind)]

    async def wait(self, kind: type[E], pred: Callable[[E], bool] = lambda _: True, timeout: float = 5) -> E:
        async with asyncio.timeout(timeout), self._changed:
            while True:
                for e in self.events:
                    if isinstance(e, kind) and pred(e):
                        return e
                await self._changed.wait()

    async def stop(self) -> None:
        self._task.cancel()


class RejectAbove(ApproveAll):
    """Tiny risk gate for tests: rejects volumes above ``limit``."""

    def __init__(self, limit: Decimal) -> None:
        self.limit = limit

    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        if intent.volume > self.limit:
            return RiskVerdict.reject("max_volume", f"{intent.volume} > {self.limit}")
        return RiskVerdict.approve()


@dataclass
class Env:
    paper: PaperBroker
    broker: ScriptedBroker
    source: FakeTickSource
    oms: OrderManager
    runtime: AccountRuntime
    rec: Recorder
    store: OmsStore
    bid: Decimal = field(default_factory=lambda: Decimal("1.10000"))
    extra: dict[str, object] = field(default_factory=dict)

    async def quote(self, bid: str | Decimal, pause: float = 0.05) -> None:
        self.bid = D(bid)
        self.source.push_quote("EURUSD", self.bid, self.bid + D("0.00010"))
        await asyncio.sleep(pause)

    async def settle(self, seconds: float = 0.1) -> None:
        await asyncio.sleep(seconds)

    async def close(self) -> None:
        await self.rec.stop()
        await self.runtime.stop()


def settings(**kw: object) -> OmsSettings:
    fields: dict[str, object] = {
        "account_id": "acct",
        "ack_timeout": timedelta(milliseconds=300),
        "unknown_grace": timedelta(milliseconds=50),
        "retry_backoff": timedelta(milliseconds=20),
        "timer_interval": timedelta(milliseconds=20),
        "min_modify_interval": timedelta(0),
    }
    fields.update(kw)
    return OmsSettings(**fields)  # type: ignore[arg-type]


async def make_env(
    *,
    store: OmsStore | None = None,
    risk: RiskGate | None = None,
    paper: PaperBroker | None = None,
    source: FakeTickSource | None = None,
    caps: dict[str, object] | None = None,
    oms_settings: OmsSettings | None = None,
    paper_settings: PaperSettings | None = None,
) -> Env:
    clock = SystemClock()
    if paper is None:
        source = FakeTickSource([eurusd()], clock)
        paper = PaperBroker(source, paper_settings or PaperSettings(account_id="acct"), clock)
        source.push_quote("EURUSD", D("1.10000"), D("1.10010"))
    assert source is not None
    broker = ScriptedBroker(paper, **(caps or {}))
    store = store or MemoryStore()
    oms = OrderManager(broker, risk or ApproveAll(), store, clock, oms_settings or settings())
    runtime = AccountRuntime("acct", broker, oms, symbols=["EURUSD"])
    rec = Recorder(oms)
    await runtime.start(warm_up=False)
    await asyncio.sleep(0.05)
    return Env(paper=paper, broker=broker, source=source, oms=oms, runtime=runtime, rec=rec, store=store)


def buy(volume: str = "0.10", **kw: object) -> OrderIntent:
    from datetime import UTC
    from datetime import datetime as dt

    fields: dict[str, object] = {
        "symbol": "EURUSD",
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "volume": D(volume),
        "strategy_id": "ema",
        "strategy_version": "2",
        "signal_ts": dt.now(UTC),
    }
    fields.update(kw)
    return OrderIntent(**fields)  # type: ignore[arg-type]


@dataclass
class Node:
    """One 'process': an OMS + runtime over some broker. ``abort`` kills it like a crash."""

    broker: ScriptedBroker
    oms: OrderManager
    runtime: AccountRuntime
    rec: Recorder

    async def crash(self, timeout: float = 5) -> None:
        """Wait until the scripted crash point is reached, then die without draining or persisting."""
        await asyncio.wait_for(self.broker.crashed.wait(), timeout)
        await self.runtime.abort()
        await self.rec.stop()

    async def close(self) -> None:
        await self.rec.stop()
        await self.runtime.stop()


async def boot(broker: Broker, store: OmsStore, oms_settings: OmsSettings | None = None) -> Node:
    scripted = broker if isinstance(broker, ScriptedBroker) else ScriptedBroker(broker)
    oms = OrderManager(scripted, ApproveAll(), store, SystemClock(), oms_settings or settings())
    runtime = AccountRuntime("acct", scripted, oms, symbols=["EURUSD"])
    rec = Recorder(oms)
    await runtime.start(warm_up=False)
    return Node(broker=scripted, oms=oms, runtime=runtime, rec=rec)
