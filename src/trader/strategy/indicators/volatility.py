"""ATR, Bollinger Bands, Donchian channel, SuperTrend."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from trader.domain.models import Bar

from .base import TWO, ZERO, Indicator, PriceIndicator, RollingExtreme, RollingSum, Source, Wilder, true_range


class ATR(Indicator[Decimal]):
    """Wilder's average true range (the first bar's true range is its high - low)."""

    def __init__(self, period: int = 14) -> None:
        self.period = period
        self._avg = Wilder(period)
        self._prev_close: Decimal | None = None

    def update(self, bar: Bar) -> None:
        tr = true_range(bar, self._prev_close)
        self._prev_close = bar.close
        self.value = self._avg.push(tr)


@dataclass(frozen=True, slots=True)
class Bands:
    upper: Decimal
    middle: Decimal
    lower: Decimal

    @property
    def width(self) -> Decimal:
        return self.upper - self.lower


class BollingerBands(PriceIndicator[Bands]):
    """SMA +/- ``k`` population standard deviations over ``period``."""

    def __init__(self, period: int = 20, k: Decimal = TWO, source: Source = "close") -> None:
        super().__init__(source)
        self.period, self.k = period, k
        self._sum = RollingSum(period)

    def update_price(self, value: Decimal) -> None:
        self._sum.push(value)
        if not self._sum.full:
            return
        n = self.period
        mean = self._sum.total / n
        variance = max(self._sum.squares / n - mean * mean, ZERO)  # clamp rounding noise
        dev = self.k * variance.sqrt()
        self.value = Bands(upper=mean + dev, middle=mean, lower=mean - dev)


class Donchian(Indicator[Bands]):
    """Highest high / lowest low of the last ``period`` bars. ``exclude_current=True`` uses the ``period``
    bars *before* the latest one, which is what a breakout test compares the latest close against."""

    def __init__(self, period: int = 20, *, exclude_current: bool = False) -> None:
        self.period = period
        self.exclude_current = exclude_current
        self._hh = RollingExtreme(period, highest=True)
        self._ll = RollingExtreme(period, highest=False)
        self._pending: Bar | None = None

    def update(self, bar: Bar) -> None:
        feed: Bar | None = bar
        if self.exclude_current:
            feed, self._pending = self._pending, bar
        if feed is None:
            return
        self._hh.push(feed.high)
        self._ll.push(feed.low)
        if self._hh.full:
            hi, lo = self._hh.value, self._ll.value
            self.value = Bands(upper=hi, middle=(hi + lo) / TWO, lower=lo)


@dataclass(frozen=True, slots=True)
class SuperTrendValue:
    line: Decimal
    direction: int  # +1 up (line below price), -1 down (line above price)


class SuperTrend(Indicator[SuperTrendValue]):
    """Bands at hl2 +/- ``multiplier`` x ATR that only tighten while the trend holds; the trend flips when the
    close crosses the opposite band. Starts in an up trend."""

    def __init__(self, period: int = 10, multiplier: Decimal = Decimal(3)) -> None:
        self._atr = ATR(period)
        self.multiplier = multiplier
        self._upper: Decimal | None = None
        self._lower: Decimal | None = None
        self._prev_close: Decimal | None = None
        self._direction = 1

    def update(self, bar: Bar) -> None:
        self._atr.update(bar)
        atr, prev_close = self._atr.value, self._prev_close
        self._prev_close = bar.close
        if atr is None:
            return
        mid = (bar.high + bar.low) / TWO
        basic_upper, basic_lower = mid + self.multiplier * atr, mid - self.multiplier * atr
        upper, lower = self._upper, self._lower
        if upper is None or lower is None or prev_close is None:
            upper, lower = basic_upper, basic_lower
        else:
            upper = basic_upper if basic_upper < upper or prev_close > upper else upper
            lower = basic_lower if basic_lower > lower or prev_close < lower else lower
        if self._direction > 0 and bar.close < lower:
            self._direction = -1
        elif self._direction < 0 and bar.close > upper:
            self._direction = 1
        self._upper, self._lower = upper, lower
        self.value = SuperTrendValue(line=lower if self._direction > 0 else upper, direction=self._direction)
