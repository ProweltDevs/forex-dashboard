"""Moving averages: SMA, EMA, WMA."""

from __future__ import annotations

from decimal import Decimal

from .base import ZERO, EmaCore, PriceIndicator, RollingSum, Source, require_period


class SMA(PriceIndicator[Decimal]):
    def __init__(self, period: int, source: Source = "close") -> None:
        super().__init__(source)
        self.period = period
        self._sum = RollingSum(period)

    def update_price(self, value: Decimal) -> None:
        self._sum.push(value)
        if self._sum.full:
            self.value = self._sum.total / self.period


class EMA(PriceIndicator[Decimal]):
    """Seeded with the SMA of the first ``period`` values, then alpha = 2 / (period + 1)."""

    def __init__(self, period: int, source: Source = "close") -> None:
        super().__init__(source)
        self.period = period
        self._core = EmaCore(period)

    def update_price(self, value: Decimal) -> None:
        self.value = self._core.push(value)


class WMA(PriceIndicator[Decimal]):
    """Linearly weighted: the newest value has weight ``period``, the oldest 1. O(1) via
    ``W' = W + n * x_new - S`` and ``S' = S + x_new - x_old``."""

    def __init__(self, period: int, source: Source = "close") -> None:
        super().__init__(source)
        require_period("period", period)
        self.period = period
        self._sum = RollingSum(period)
        self._weighted = ZERO
        self._denominator = Decimal(period * (period + 1) // 2)

    def update_price(self, value: Decimal) -> None:
        n = self.period
        before = self._sum.total
        count = len(self._sum.window)
        if count < n:
            self._weighted += (count + 1) * value
        else:
            self._weighted += n * value - before
        self._sum.push(value)
        if self._sum.full:
            self.value = self._weighted / self._denominator
