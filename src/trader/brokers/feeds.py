"""Market-data sources for the paper broker (``MarketDataSource``):

- ``BrokerFeed``: a real broker account used for prices only (``paper.market_data_from``). It never trades.
- ``SyntheticFeed``: a seeded random walk (``paper.synthetic_feed``) so paper mode runs without broker
  credentials. It is NOT market data: use it to exercise the platform, never to judge a strategy.

Both fan ticks out to every consumer, so several paper accounts can share one source.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import random
import time
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from trader.domain.enums import Timeframe
from trader.domain.errors import ConfigError
from trader.domain.events import ConnectionEvent, TickEvent
from trader.domain.models import Bar, Instrument, Tick
from trader.domain.time import Clock
from trader.observability.logging import get_logger

from .base import Broker

_log = get_logger("trader.feeds")


class _FanOut:
    def __init__(self) -> None:
        self._queues: list[asyncio.Queue[Tick | None]] = []

    def _publish(self, tick: Tick) -> None:
        for q in self._queues:
            q.put_nowait(tick)

    def _end(self) -> None:
        for q in self._queues:
            q.put_nowait(None)

    async def ticks(self) -> AsyncIterator[Tick]:
        q: asyncio.Queue[Tick | None] = asyncio.Queue()
        self._queues.append(q)
        try:
            while (tick := await q.get()) is not None:
                yield tick
        finally:
            self._queues.remove(q)


class BrokerFeed(_FanOut):
    """Prices from a real broker account (demo or live) for paper trading; only market-data calls are made."""

    def __init__(self, broker: Broker) -> None:
        super().__init__()
        self.broker = broker
        self._pump: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self.broker.connect()
        self._pump = asyncio.create_task(self._run(), name=f"feed-{self.broker.name}")

    async def close(self) -> None:
        if self._pump is not None:
            self._pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump
            self._pump = None
        await self.broker.close()
        self._end()

    async def _run(self) -> None:
        async for event in self.broker.events():
            if isinstance(event, TickEvent):
                self._publish(event.tick)
            elif isinstance(event, ConnectionEvent):
                _log.info(
                    "feed_connection", broker=self.broker.name, state=str(event.state), reason=event.reason
                )

    async def instruments(self) -> list[Instrument]:
        return await self.broker.instruments()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        await self.broker.subscribe_ticks(symbols)

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        return await self.broker.candles(symbol, tf, start, end)


# ------------------------------------------------------------------------------------------- synthetic


def _fx(symbol: str, digits: int, price: str, spread: str) -> tuple[Instrument, Decimal, Decimal]:
    tick = Decimal(1).scaleb(-digits)
    inst = Instrument(
        symbol=symbol,
        broker_symbol=symbol,
        broker_id=symbol,
        digits=digits,
        tick_size=tick,
        contract_size=Decimal(100_000),
        min_volume=Decimal("0.01"),
        max_volume=Decimal(100),
        volume_step=Decimal("0.01"),
        base_currency=symbol[:3],
        quote_currency=symbol[3:],
        margin_currency=symbol[:3],
    )
    return inst, Decimal(price), Decimal(spread)


SYNTHETIC: dict[str, tuple[Instrument, Decimal, Decimal]] = {
    "EURUSD": _fx("EURUSD", 5, "1.08000", "0.00010"),
    "GBPUSD": _fx("GBPUSD", 5, "1.27000", "0.00012"),
    "AUDUSD": _fx("AUDUSD", 5, "0.66000", "0.00012"),
    "USDCHF": _fx("USDCHF", 5, "0.88000", "0.00012"),
    "USDJPY": _fx("USDJPY", 3, "150.000", "0.012"),
    "XAUUSD": (
        Instrument(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            broker_id="XAUUSD",
            digits=2,
            tick_size=Decimal("0.01"),
            contract_size=Decimal(100),
            min_volume=Decimal("0.01"),
            max_volume=Decimal(50),
            volume_step=Decimal("0.01"),
            base_currency="XAU",
            quote_currency="USD",
            margin_currency="USD",
        ),
        Decimal("2400.00"),
        Decimal("0.30"),
    ),
}


class SyntheticFeed(_FanOut):
    """Seeded geometric random walk per symbol, one tick per symbol every ``interval``. ``volatility`` is the
    per-tick standard deviation of the log return. No candle history (strategies warm up from live bars)."""

    def __init__(
        self,
        symbols: Sequence[str],
        clock: Clock,
        *,
        seed: int = 1,
        interval: timedelta = timedelta(milliseconds=250),
        volatility: float = 0.00005,
    ) -> None:
        super().__init__()
        unknown = [s for s in symbols if s not in SYNTHETIC]
        if unknown:
            raise ConfigError(f"synthetic feed has no definition for {unknown}; known: {sorted(SYNTHETIC)}")
        self._symbols = tuple(symbols)
        self._clock = clock
        self._rng = random.Random(seed)  # deterministic test data, not used for anything security related
        self._interval = interval
        self._vol = volatility
        self._mid = {s: float(SYNTHETIC[s][1]) for s in self._symbols}
        self._subscribed: set[str] = set()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="synthetic-feed")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._end()

    def next_tick(self, symbol: str) -> Tick:
        inst, _, spread = SYNTHETIC[symbol]
        self._mid[symbol] *= math.exp(self._rng.gauss(0.0, self._vol))
        mid = inst.round_price(Decimal(repr(self._mid[symbol])))
        bid = inst.round_price(mid - spread / 2)
        now = self._clock.now()
        return Tick(
            symbol=symbol, bid=bid, ask=bid + spread, ts=now, recv_ts=now, recv_mono_ns=time.monotonic_ns()
        )

    async def _run(self) -> None:
        while True:
            for symbol in self._symbols:
                if symbol in self._subscribed:
                    self._publish(self.next_tick(symbol))
            await asyncio.sleep(self._interval.total_seconds())

    async def instruments(self) -> list[Instrument]:
        return [SYNTHETIC[s][0] for s in self._symbols]

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self._subscribed.update(s for s in symbols if s in self._symbols)

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        return []
