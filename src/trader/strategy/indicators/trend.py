"""ADX / DMI (Wilder)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from trader.domain.models import Bar

from .base import HUNDRED, ZERO, Indicator, Wilder, require_period, true_range


@dataclass(frozen=True, slots=True)
class AdxValue:
    adx: Decimal
    plus_di: Decimal
    minus_di: Decimal


class _WilderSum:
    """Wilder's running total: the first value is the sum of the first ``n`` inputs, then
    ``s = s - s / n + x`` (the DI ratios are the same as with the smoothed average)."""

    def __init__(self, n: int) -> None:
        self.n = n
        self._seed: list[Decimal] = []
        self.value: Decimal | None = None

    def push(self, x: Decimal) -> Decimal | None:
        if self.value is None:
            self._seed.append(x)
            if len(self._seed) == self.n:
                self.value = sum(self._seed, ZERO)
        else:
            self.value = self.value - self.value / self.n + x
        return self.value


class ADX(Indicator[AdxValue]):
    """Ready after ``2 x period`` bars: ``period`` moves for the DIs, then ``period`` DX values."""

    def __init__(self, period: int = 14) -> None:
        require_period("period", period)
        self.period = period
        self._tr, self._plus, self._minus = _WilderSum(period), _WilderSum(period), _WilderSum(period)
        self._adx = Wilder(period)
        self._prev: Bar | None = None

    def update(self, bar: Bar) -> None:
        prev, self._prev = self._prev, bar
        if prev is None:
            return
        up, down = bar.high - prev.high, prev.low - bar.low
        plus_dm = up if up > down and up > 0 else ZERO
        minus_dm = down if down > up and down > 0 else ZERO
        tr = self._tr.push(true_range(bar, prev.close))
        plus, minus = self._plus.push(plus_dm), self._minus.push(minus_dm)
        if tr is None or plus is None or minus is None:
            return
        plus_di = HUNDRED * plus / tr if tr > 0 else ZERO
        minus_di = HUNDRED * minus / tr if tr > 0 else ZERO
        total = plus_di + minus_di
        dx = HUNDRED * abs(plus_di - minus_di) / total if total > 0 else ZERO
        adx = self._adx.push(dx)
        if adx is not None:
            self.value = AdxValue(adx=adx, plus_di=plus_di, minus_di=minus_di)
