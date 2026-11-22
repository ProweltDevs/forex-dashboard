"""RSI, MACD, Stochastic."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from trader.domain.models import Bar

from .base import (
    HUNDRED,
    ZERO,
    EmaCore,
    Indicator,
    PriceIndicator,
    RollingExtreme,
    RollingSum,
    Source,
    Wilder,
)


class RSI(PriceIndicator[Decimal]):
    """Wilder's RSI. 100 when there were no losses in the window, 50 when the price did not move."""

    def __init__(self, period: int = 14, source: Source = "close") -> None:
        super().__init__(source)
        self.period = period
        self._gain = Wilder(period)
        self._loss = Wilder(period)
        self._prev: Decimal | None = None

    def update_price(self, value: Decimal) -> None:
        prev, self._prev = self._prev, value
        if prev is None:
            return
        change = value - prev
        gain = self._gain.push(max(change, ZERO))
        loss = self._loss.push(max(-change, ZERO))
        if gain is None or loss is None:
            return
        if loss == 0:
            self.value = Decimal(50) if gain == 0 else HUNDRED
        else:
            self.value = HUNDRED - HUNDRED / (1 + gain / loss)


@dataclass(frozen=True, slots=True)
class MacdValue:
    macd: Decimal
    signal: Decimal
    histogram: Decimal


class MACD(PriceIndicator[MacdValue]):
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9, source: Source = "close") -> None:
        super().__init__(source)
        if fast >= slow:
            raise ValueError("fast period must be shorter than slow")
        self._fast, self._slow, self._signal = EmaCore(fast), EmaCore(slow), EmaCore(signal)

    def update_price(self, value: Decimal) -> None:
        fast, slow = self._fast.push(value), self._slow.push(value)
        if fast is None or slow is None:
            return
        macd = fast - slow
        signal = self._signal.push(macd)
        if signal is not None:
            self.value = MacdValue(macd=macd, signal=signal, histogram=macd - signal)


@dataclass(frozen=True, slots=True)
class StochValue:
    k: Decimal
    d: Decimal


class Stochastic(Indicator[StochValue]):
    """%K = 100 x (close - lowest low) / (highest high - lowest low) over ``k_period`` (50 for a flat
    range), smoothed by ``smooth`` (1 = fast stochastic); %D = SMA(``d_period``) of %K."""

    def __init__(self, k_period: int = 14, d_period: int = 3, smooth: int = 1) -> None:
        self._hh = RollingExtreme(k_period, highest=True)
        self._ll = RollingExtreme(k_period, highest=False)
        self._smooth = RollingSum(smooth)
        self._d = RollingSum(d_period)

    def update(self, bar: Bar) -> None:
        self._hh.push(bar.high)
        self._ll.push(bar.low)
        if not self._hh.full:
            return
        hh, ll = self._hh.value, self._ll.value
        raw = Decimal(50) if hh == ll else HUNDRED * (bar.close - ll) / (hh - ll)
        self._smooth.push(raw)
        if not self._smooth.full:
            return
        k = self._smooth.total / self._smooth.n
        self._d.push(k)
        if self._d.full:
            self.value = StochValue(k=k, d=self._d.total / self._d.n)
