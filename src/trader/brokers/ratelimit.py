"""Client-side token bucket. Adapters ``await acquire()`` before each request so we never hit broker
limits; the paper broker uses ``try_acquire()`` to emulate a broker that rejects with ``RateLimited``."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from decimal import ROUND_CEILING, Decimal

from .base import RateLimit

_NS = Decimal(1_000_000_000)


class TokenBucket:
    __slots__ = ("_last_ns", "_now_ns", "_sleep", "_tokens", "burst", "rate_per_s")

    def __init__(
        self,
        limit: RateLimit,
        *,
        now_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.rate_per_s = limit.rate_per_s
        self.burst = limit.burst
        self._now_ns = now_ns
        self._sleep = sleep
        self._tokens = Decimal(limit.burst)
        self._last_ns = now_ns()

    def _refill(self) -> None:
        now = self._now_ns()
        elapsed = max(0, now - self._last_ns)
        self._last_ns = now
        self._tokens = min(Decimal(self.burst), self._tokens + Decimal(elapsed) * self.rate_per_s / _NS)

    @property
    def tokens(self) -> Decimal:
        self._refill()
        return self._tokens

    def try_acquire(self, n: int = 1) -> int:
        """Take ``n`` tokens. Return 0 on success, otherwise the wait in ms until they would be available."""
        if n > self.burst:
            raise ValueError(f"cannot acquire {n} tokens from a bucket of {self.burst}")
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return 0
        missing = Decimal(n) - self._tokens
        return int((missing * 1000 / self.rate_per_s).to_integral_value(rounding=ROUND_CEILING))

    async def acquire(self, n: int = 1) -> None:
        """Wait until ``n`` tokens are available, then take them."""
        while (wait_ms := self.try_acquire(n)) > 0:
            await self._sleep(wait_ms / 1000)


class SlidingWindow:
    """At most ``limit`` acquisitions in any ``window_s``-second window (exact, unlike a token bucket,
    which can allow up to 2x ``limit`` inside one window). For brokers that publish "N per window" limits."""

    __slots__ = ("_events", "_now_ns", "_sleep", "limit", "window_ns")

    def __init__(
        self,
        limit: int,
        window_s: Decimal,
        *,
        now_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if limit <= 0 or window_s <= 0:
            raise ValueError("limit and window must be > 0")
        self.limit = limit
        self.window_ns = int(window_s * _NS)
        self._now_ns = now_ns
        self._sleep = sleep
        self._events: deque[int] = deque()

    def try_acquire(self) -> int:
        """Record one request and return 0, or return the wait in ms until one is allowed."""
        now = self._now_ns()
        while self._events and now - self._events[0] >= self.window_ns:
            self._events.popleft()
        if len(self._events) < self.limit:
            self._events.append(now)
            return 0
        wait_ns = self.window_ns - (now - self._events[0])
        return max(1, -(-wait_ns // 1_000_000))

    async def acquire(self) -> None:
        while (wait_ms := self.try_acquire()) > 0:
            await self._sleep(wait_ms / 1000)
