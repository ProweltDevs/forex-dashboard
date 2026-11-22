"""Market-condition trackers fed with every tick (cheap, synchronous): typical spread per symbol, last tick
time per symbol, and the offset between our receive clock and the broker's tick timestamps."""

from __future__ import annotations

from collections import deque
from datetime import datetime
from decimal import Decimal

from trader.domain.models import Tick


class SpreadTracker:
    """Typical spread = exponential moving average of the spread. Each sample is clipped at
    ``spike_factor`` x the current average, so a news spike barely moves it, while a lasting change in the
    spread still pulls the average along."""

    def __init__(
        self, *, alpha: Decimal = Decimal("0.02"), min_samples: int = 50, spike_factor: Decimal = Decimal(5)
    ) -> None:
        self._alpha = alpha
        self._min = min_samples
        self._spike = spike_factor
        self._ema: dict[str, Decimal] = {}
        self._n: dict[str, int] = {}

    def on_tick(self, tick: Tick) -> None:
        spread = tick.ask - tick.bid
        if spread <= 0:
            return
        sym = tick.symbol
        ema = self._ema.get(sym)
        if ema is None:
            self._ema[sym] = spread
        else:
            self._ema[sym] = ema + self._alpha * (min(spread, ema * self._spike) - ema)
        self._n[sym] = self._n.get(sym, 0) + 1

    def typical(self, symbol: str) -> Decimal | None:
        """``None`` until ``min_samples`` ticks were seen (the rule then uses its absolute cap only)."""
        return self._ema.get(symbol) if self._n.get(symbol, 0) >= self._min else None


class FeedClock:
    """Receive time of the last tick per symbol."""

    def __init__(self) -> None:
        self.last: dict[str, datetime] = {}

    def on_tick(self, tick: Tick) -> None:
        self.last[tick.symbol] = tick.recv_ts


class ClockDrift:
    """Median of (receive time - broker timestamp) over recent ticks, in ms. It includes network latency,
    so the threshold must be well above normal latency (default 2 s)."""

    def __init__(self, window: int = 64) -> None:
        self._samples: deque[float] = deque(maxlen=window)

    def on_tick(self, tick: Tick) -> None:
        if tick.broker_ts is not None:
            self._samples.append((tick.recv_ts - tick.broker_ts).total_seconds() * 1000)

    def offset_ms(self) -> float | None:
        if len(self._samples) < 8:
            return None
        ordered = sorted(self._samples)
        return ordered[len(ordered) // 2]
