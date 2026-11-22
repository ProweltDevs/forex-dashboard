"""In-process fan-out bus (SPEC §2).

``publish`` is synchronous and never awaits, so the trading path can never be blocked by a subscriber.
Each subscriber has its own queue and overflow policy:

- ``LOSSLESS``: never drops (OMS, strategies, tick recorder). Grows past ``maxsize`` with a warning
  each time it crosses the high-water mark; a consumer that falls behind costs memory, not data.
- ``DROP_OLDEST``: bounded; the oldest item is discarded when full (counted).
- ``COALESCE``: keeps only the latest item per key (UI streams: the latest tick per symbol).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable, Hashable
from enum import StrEnum

from trader.domain.events import TickEvent
from trader.domain.models import Tick
from trader.observability.logging import get_logger

_log = get_logger("trader.marketdata.bus")


class Overflow(StrEnum):
    LOSSLESS = "lossless"
    DROP_OLDEST = "drop_oldest"
    COALESCE = "coalesce"


class SubscriptionClosed(Exception):
    pass


class Subscription[T]:
    """One consumer's queue. Single consumer: iterate it (``async for``) or call ``get()``."""

    def __init__(
        self,
        name: str,
        policy: Overflow,
        maxsize: int,
        key: Callable[[T], Hashable] | None,
        accept: Callable[[T], bool] | None,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")
        if policy is Overflow.COALESCE and key is None:
            raise ValueError("COALESCE needs a key function")
        self.name = name
        self.policy = policy
        self.maxsize = maxsize
        self._key = key
        self._accept = accept
        self._queue: deque[T] = deque(maxlen=maxsize if policy is Overflow.DROP_OLDEST else None)
        self._latest: dict[Hashable, T] = {}
        self._ready = asyncio.Event()
        self._closed = False
        self._above_high_water = False
        self.dropped = 0
        self.delivered = 0

    def __len__(self) -> int:
        return len(self._latest) if self.policy is Overflow.COALESCE else len(self._queue)

    @property
    def closed(self) -> bool:
        return self._closed

    def offer(self, item: T) -> None:
        """Never blocks, never raises for a full queue."""
        if self._closed or (self._accept is not None and not self._accept(item)):
            return
        if self.policy is Overflow.COALESCE:
            assert self._key is not None
            k = self._key(item)
            if k in self._latest:
                self.dropped += 1  # superseded
                del self._latest[k]  # re-insert so the newest update goes to the back
            self._latest[k] = item
        else:
            if self.policy is Overflow.DROP_OLDEST and len(self._queue) == self.maxsize:
                self.dropped += 1
            self._queue.append(item)
            if self.policy is Overflow.LOSSLESS:
                self._check_high_water()
        self._ready.set()

    def _check_high_water(self) -> None:
        depth = len(self._queue)
        if depth > self.maxsize and not self._above_high_water:
            self._above_high_water = True
            _log.warning("bus_consumer_lagging", subscriber=self.name, depth=depth, high_water=self.maxsize)
        elif depth <= self.maxsize // 2 and self._above_high_water:
            self._above_high_water = False
            _log.info("bus_consumer_caught_up", subscriber=self.name, depth=depth)

    def get_nowait(self) -> T | None:
        if self.policy is Overflow.COALESCE:
            if not self._latest:
                return None
            k = next(iter(self._latest))
            item = self._latest.pop(k)
        else:
            if not self._queue:
                return None
            item = self._queue.popleft()
            if self.policy is Overflow.LOSSLESS and self._above_high_water:
                self._check_high_water()
        self.delivered += 1
        return item

    async def get(self) -> T:
        while True:
            item = self.get_nowait()
            if item is not None:
                return item
            if self._closed:
                raise SubscriptionClosed(self.name)
            self._ready.clear()
            await self._ready.wait()

    def close(self) -> None:
        self._closed = True
        self._ready.set()

    async def __aiter__(self) -> AsyncIterator[T]:
        while True:
            try:
                yield await self.get()
            except SubscriptionClosed:
                return


class Bus[T]:
    def __init__(self) -> None:
        self._subs: list[Subscription[T]] = []

    def subscribe(
        self,
        name: str,
        *,
        policy: Overflow = Overflow.LOSSLESS,
        maxsize: int = 10_000,
        key: Callable[[T], Hashable] | None = None,
        accept: Callable[[T], bool] | None = None,
    ) -> Subscription[T]:
        sub = Subscription(name, policy, maxsize, key, accept)
        self._subs.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription[T]) -> None:
        sub.close()
        if sub in self._subs:
            self._subs.remove(sub)

    @property
    def subscribers(self) -> tuple[Subscription[T], ...]:
        return tuple(self._subs)

    def publish(self, item: T) -> None:
        for sub in self._subs:
            sub.offer(item)

    def close(self) -> None:
        for sub in self._subs:
            sub.close()
        self._subs.clear()


class TickBus(Bus[TickEvent]):
    """Tick fan-out plus the per-symbol last-tick cache strategies read via ``ctx.last_tick``."""

    def __init__(self) -> None:
        super().__init__()
        self._last: dict[tuple[str, str], Tick] = {}
        self._last_any: dict[str, Tick] = {}

    def publish(self, item: TickEvent) -> None:
        tick = item.tick
        key = (item.broker, tick.symbol)
        prev = self._last.get(key)
        if prev is not None and tick.ts < prev.ts:
            return  # out-of-order tick: never regress the cache or downstream consumers
        self._last[key] = tick
        self._last_any[tick.symbol] = tick
        super().publish(item)

    def last(self, symbol: str, broker: str | None = None) -> Tick | None:
        if broker is None:
            return self._last_any.get(symbol)
        return self._last.get((broker, symbol))

    def coalesced(self, name: str, *, maxsize: int = 1_000) -> Subscription[TickEvent]:
        """Latest tick per (broker, symbol): for UI/API streams that must never fall behind."""
        return self.subscribe(
            name, policy=Overflow.COALESCE, maxsize=maxsize, key=lambda e: (e.broker, e.tick.symbol)
        )
