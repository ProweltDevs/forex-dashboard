"""Adapter-neutral harness for the broker contract suite (docs/SPEC.md §4).

Each adapter provides a ``ContractHarness``: a connected, reconciled broker, a symbol to trade, and hooks
for the few things the Broker protocol cannot express (forcing a disconnect). Real adapters (phases 2-3)
add a harness factory here and are run with ``-m demo`` against DEMO accounts only.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypeVar

from trader.brokers.base import Broker, RateLimit
from trader.brokers.paper import FakeTickSource, PaperBroker, PaperSettings
from trader.domain import Instrument, OrderType, Side, Tick, TickEvent
from trader.domain.events import DomainEvent, FillEvent
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest
from trader.domain.time import SystemClock

E = TypeVar("E")
TIMEOUT_S = 5.0
CONTRACT_STRATEGY = "contract-suite"


class EventRecorder:
    """Consumes ``broker.events()`` in the background and lets tests wait for specific events."""

    def __init__(self, broker: Broker) -> None:
        self.events: list[DomainEvent] = []
        self._changed = asyncio.Condition()
        self._task = asyncio.create_task(self._run(broker))

    async def _run(self, broker: Broker) -> None:
        async for event in broker.events():
            async with self._changed:
                self.events.append(event)
                self._changed.notify_all()

    async def wait_for(
        self,
        kind: type[E],
        predicate: Callable[[E], bool] = lambda _: True,
        *,
        after: int = 0,
        timeout: float = TIMEOUT_S,
    ) -> E:
        """Return the first event of ``kind`` (at index >= ``after``) matching ``predicate``."""

        def find() -> E | None:
            for event in self.events[after:]:
                if isinstance(event, kind) and predicate(event):
                    return event
            return None

        async with asyncio.timeout(timeout), self._changed:
            while (found := find()) is None:
                await self._changed.wait()
            return found

    def of(self, kind: type[E], after: int = 0) -> list[E]:
        return [e for e in self.events[after:] if isinstance(e, kind)]

    @property
    def mark(self) -> int:
        return len(self.events)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


@dataclass
class ContractHarness:
    broker: Broker
    symbol: str
    recorder: EventRecorder
    force_disconnect: Callable[[], Awaitable[None]]
    reconnect: Callable[[], Awaitable[None]]
    teardown: Callable[[], Awaitable[None]]
    placed: list[str] = field(default_factory=list)  # client_order_ids sent by the test

    async def instrument(self) -> Instrument:
        return next(i for i in await self.broker.instruments() if i.symbol == self.symbol)

    async def last_tick(self) -> Tick:
        event = await self.recorder.wait_for(TickEvent, lambda e: e.tick.symbol == self.symbol)
        ticks = [e for e in self.recorder.of(TickEvent) if e.tick.symbol == self.symbol]
        return ticks[-1].tick if ticks else event.tick

    def request(self, side: Side, volume: Decimal, **kwargs: object) -> OrderRequest:
        cid = new_client_order_id()
        self.placed.append(cid)
        fields: dict[str, object] = {
            "client_order_id": cid,
            "symbol": self.symbol,
            "side": side,
            "order_type": OrderType.MARKET,
            "volume": volume,
            "strategy_id": CONTRACT_STRATEGY,
            "strategy_version": "1",
            "signal_ts": datetime.now(UTC),
        }
        fields.update(kwargs)
        return OrderRequest(**fields)  # type: ignore[arg-type]

    async def fill_for(self, client_order_id: str, after: int = 0) -> FillEvent:
        return await self.recorder.wait_for(
            FillEvent, lambda e: e.fill.client_order_id == client_order_id, after=after
        )

    async def cleanup(self) -> None:
        """Leave the account as we found it: cancel our orders and close positions our orders opened."""
        broker = self.broker
        with contextlib.suppress(Exception):
            ours = set(self.placed)
            for order in await broker.open_orders():
                if order.client_order_id in ours and order.broker_order_id:
                    await broker.cancel_order(order.broker_order_id)
            opened = {
                e.fill.position_id for e in self.recorder.of(FillEvent) if e.fill.client_order_id in ours
            }
            for pos in await broker.positions():
                if pos.position_id in opened:
                    await broker.close_position(pos.position_id)
        await self.recorder.stop()
        await self.teardown()


# ---------------------------------------------------------------------------------------------- paper


def paper_instruments() -> list[Instrument]:
    return [
        Instrument(
            symbol="EURUSD",
            broker_symbol="EURUSD",
            broker_id="1",
            digits=5,
            tick_size=Decimal("0.00001"),
            contract_size=Decimal(100_000),
            min_volume=Decimal("0.01"),
            max_volume=Decimal(50),
            volume_step=Decimal("0.01"),
            base_currency="EUR",
            quote_currency="USD",
            margin_currency="EUR",
        ),
    ]


async def paper_harness() -> ContractHarness:
    clock = SystemClock()
    source = FakeTickSource(paper_instruments(), clock, base_prices={"EURUSD": Decimal("1.10000")})
    broker = PaperBroker(
        source,
        PaperSettings(
            account_id="contract-paper",
            rate_limit=RateLimit(name="requests", rate_per_s=Decimal(200), burst=40),
        ),
        clock,
    )

    async def ticker() -> None:
        # Deterministic oscillation: 1.10000 / 1.10002 bid, 1 pip spread.
        for i in range(1_000_000):
            bid = Decimal("1.10000") + Decimal("0.00002") * (i % 2)
            source.push_quote("EURUSD", bid, bid + Decimal("0.00010"))
            await asyncio.sleep(0.005)

    ticks = asyncio.create_task(ticker())
    await broker.connect()
    recorder = EventRecorder(broker)
    await broker.subscribe_ticks(["EURUSD"])
    await recorder.wait_for(TickEvent)
    await broker.snapshot()  # reconcile before trading, as the OMS will

    async def teardown() -> None:
        ticks.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticks
        await broker.close()
        source.close()

    return ContractHarness(
        broker=broker,
        symbol="EURUSD",
        recorder=recorder,
        force_disconnect=broker.simulate_disconnect,
        reconnect=broker.simulate_reconnect,
        teardown=teardown,
    )


HARNESSES: dict[str, Callable[[], Awaitable[ContractHarness]]] = {"paper": paper_harness}
