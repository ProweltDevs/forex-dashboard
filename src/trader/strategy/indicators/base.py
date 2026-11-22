"""Indicator building blocks. Every indicator is incremental: ``update(bar)`` costs O(1) (amortized for
rolling highs/lows), ``value`` is ``None`` until ``ready``. Values are Decimal, like prices.

Price-based indicators also accept a single price with ``update_price`` (e.g. a tick's mid).
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from typing import Literal

from trader.domain.models import Bar

type Source = Literal["open", "high", "low", "close", "hl2", "hlc3", "ohlc4"]

ZERO = Decimal(0)
ONE = Decimal(1)
TWO = Decimal(2)
THREE = Decimal(3)
HUNDRED = Decimal(100)


def price(bar: Bar, source: Source) -> Decimal:
    match source:
        case "open":
            return bar.open
        case "high":
            return bar.high
        case "low":
            return bar.low
        case "close":
            return bar.close
        case "hl2":
            return (bar.high + bar.low) / TWO
        case "hlc3":
            return (bar.high + bar.low + bar.close) / THREE
        case "ohlc4":
            return (bar.open + bar.high + bar.low + bar.close) / 4


def require_period(name: str, value: int, minimum: int = 1) -> None:
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")


class Indicator[T]:
    """Base: subclasses implement ``update``; ``value`` is None until ``ready``."""

    value: T | None = None

    @property
    def ready(self) -> bool:
        return self.value is not None

    def update(self, bar: Bar) -> None:
        raise NotImplementedError


class PriceIndicator[T](Indicator[T]):
    """An indicator of one price series (close by default)."""

    def __init__(self, source: Source = "close") -> None:
        self.source: Source = source

    def update(self, bar: Bar) -> None:
        self.update_price(price(bar, self.source))

    def update_price(self, value: Decimal) -> None:
        raise NotImplementedError


class RollingSum:
    """Sum (and sum of squares) of the last ``n`` values."""

    def __init__(self, n: int) -> None:
        require_period("period", n)
        self.n = n
        self.window: deque[Decimal] = deque()
        self.total = ZERO
        self.squares = ZERO

    def push(self, x: Decimal) -> None:
        self.window.append(x)
        self.total += x
        self.squares += x * x
        if len(self.window) > self.n:
            old = self.window.popleft()
            self.total -= old
            self.squares -= old * old

    @property
    def full(self) -> bool:
        return len(self.window) == self.n


class RollingExtreme:
    """Max or min of the last ``n`` values with a monotonic deque (amortized O(1))."""

    def __init__(self, n: int, *, highest: bool) -> None:
        require_period("period", n)
        self.n = n
        self.highest = highest
        self._q: deque[tuple[int, Decimal]] = deque()
        self._i = 0

    def push(self, x: Decimal) -> None:
        q = self._q
        while q and (q[-1][1] <= x if self.highest else q[-1][1] >= x):
            q.pop()
        q.append((self._i, x))
        if q[0][0] <= self._i - self.n:
            q.popleft()
        self._i += 1

    @property
    def full(self) -> bool:
        return self._i >= self.n

    @property
    def value(self) -> Decimal:
        return self._q[0][1]


class Wilder:
    """Wilder smoothing: the first value is the mean of the first ``n`` inputs, then
    ``avg = (avg * (n - 1) + x) / n``."""

    def __init__(self, n: int) -> None:
        require_period("period", n)
        self.n = n
        self._seed: list[Decimal] = []
        self.value: Decimal | None = None

    def push(self, x: Decimal) -> Decimal | None:
        if self.value is None:
            self._seed.append(x)
            if len(self._seed) == self.n:
                self.value = sum(self._seed, ZERO) / self.n
                self._seed = []
        else:
            self.value = (self.value * (self.n - 1) + x) / self.n
        return self.value


class EmaCore:
    """EMA seeded with the SMA of the first ``n`` values; alpha = 2 / (n + 1)."""

    def __init__(self, n: int) -> None:
        require_period("period", n)
        self.n = n
        self.alpha = TWO / (n + 1)
        self._seed: list[Decimal] = []
        self.value: Decimal | None = None

    def push(self, x: Decimal) -> Decimal | None:
        if self.value is None:
            self._seed.append(x)
            if len(self._seed) == self.n:
                self.value = sum(self._seed, ZERO) / self.n
                self._seed = []
        else:
            self.value += self.alpha * (x - self.value)
        return self.value


def true_range(bar: Bar, prev_close: Decimal | None) -> Decimal:
    if prev_close is None:
        return bar.high - bar.low
    return max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))
