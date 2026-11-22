"""Tick -> bar aggregation (SPEC §11).

- Boundaries are exact in UTC: a bar covers ``[start, start + duration)``; a tick at exactly ``start``
  belongs to the new bar. ``day_offset`` aligns the trading day to the broker's (e.g. 22:00 UTC "NY close"
  or a UTC+2 server day): every timeframe is computed on ``ts - day_offset`` and shifted back, so H4/D1/W1/MN1
  line up with the broker's candles. Weeks start on Monday of that shifted calendar.
- Prices come from one side of the quote (default bid, like broker candles); volume is the tick count.
- Like cTrader/Exness candles, there are no bars for periods without ticks.
- A bar completes when a tick for a later bar arrives, or when ``on_time`` sees its end pass (+ grace for
  late ticks). Only complete bars are emitted; ``current()`` exposes the forming bar.
- Ticks older than the forming bar (or already-closed bars) are late: counted and ignored.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from trader.domain.enums import Timeframe
from trader.domain.models import Bar, Tick
from trader.domain.time import ensure_utc

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class PriceSource(StrEnum):
    BID = "bid"
    ASK = "ask"
    MID = "mid"

    def of(self, tick: Tick) -> Decimal:
        if self is PriceSource.BID:
            return tick.bid
        if self is PriceSource.ASK:
            return tick.ask
        return tick.mid


def bar_start(ts: datetime, tf: Timeframe, day_offset: timedelta = timedelta(0)) -> datetime:
    """Start of the ``tf`` bar containing ``ts``."""
    shifted = ensure_utc(ts) - day_offset
    duration = tf.duration
    if tf is Timeframe.MN1:
        start = shifted.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif tf is Timeframe.W1:
        day = shifted.replace(hour=0, minute=0, second=0, microsecond=0)
        start = day - timedelta(days=day.weekday())
    else:
        assert duration is not None
        start = _EPOCH + ((shifted - _EPOCH) // duration) * duration
    return start + day_offset


def bar_end(start: datetime, tf: Timeframe, day_offset: timedelta = timedelta(0)) -> datetime:
    if tf is Timeframe.MN1:
        shifted = start - day_offset
        year, month = (shifted.year + 1, 1) if shifted.month == 12 else (shifted.year, shifted.month + 1)
        return shifted.replace(year=year, month=month) + day_offset
    duration = tf.duration
    assert duration is not None
    return start + duration


@dataclass(slots=True)
class _Forming:
    start: datetime
    end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def add(self, price: Decimal) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += 1

    def to_bar(self, symbol: str, tf: Timeframe, *, complete: bool) -> Bar:
        return Bar(
            symbol=symbol,
            timeframe=tf,
            start=self.start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            complete=complete,
        )


class BarBuilder:
    def __init__(
        self,
        timeframes: Iterable[Timeframe],
        *,
        price: PriceSource = PriceSource.BID,
        day_offset: timedelta = timedelta(0),
        history: int = 1_000,
        grace: timedelta = timedelta(seconds=2),
    ) -> None:
        self.timeframes = tuple(dict.fromkeys(timeframes))
        if not self.timeframes:
            raise ValueError("at least one timeframe is required")
        if history <= 0:
            raise ValueError("history must be > 0")
        self.price = price
        self.day_offset = day_offset
        self.grace = grace
        self._history_len = history
        self._forming: dict[tuple[str, Timeframe], _Forming] = {}
        self._history: dict[tuple[str, Timeframe], deque[Bar]] = {}
        self.late_ticks = 0

    # ------------------------------------------------------------------ queries

    def current(self, symbol: str, tf: Timeframe) -> Bar | None:
        forming = self._forming.get((symbol, tf))
        return forming.to_bar(symbol, tf, complete=False) if forming else None

    def history(self, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        bars = list(self._history.get((symbol, tf), ()))
        return bars if n is None else bars[-n:]

    def _last_closed_start(self, key: tuple[str, Timeframe]) -> datetime | None:
        hist = self._history.get(key)
        return hist[-1].start if hist else None

    # ------------------------------------------------------------------ input

    def seed(self, bars: Sequence[Bar]) -> None:
        """Warm up from broker candles. Complete bars go to history (newer duplicates win); a trailing
        incomplete bar becomes the forming bar so ticks keep extending it."""
        for bar in sorted(bars, key=lambda b: b.start):
            if bar.timeframe not in self.timeframes:
                continue
            key = (bar.symbol, bar.timeframe)
            if bar.complete:
                hist = self._history.setdefault(key, deque(maxlen=self._history_len))
                if hist and hist[-1].start >= bar.start:
                    if hist[-1].start == bar.start:
                        hist[-1] = bar
                    continue
                hist.append(bar)
                forming = self._forming.get(key)
                if forming is not None and forming.start <= bar.start:
                    del self._forming[key]
            else:
                last = self._last_closed_start(key)
                if last is None or bar.start > last:
                    self._forming[key] = _Forming(
                        bar.start,
                        bar_end(bar.start, bar.timeframe, self.day_offset),
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                    )

    def on_tick(self, tick: Tick) -> list[Bar]:
        """Apply one tick; return the bars it completed (oldest timeframe first)."""
        price = self.price.of(tick)
        done: list[Bar] = []
        late = False
        for tf in self.timeframes:
            key = (tick.symbol, tf)
            start = bar_start(tick.ts, tf, self.day_offset)
            forming = self._forming.get(key)
            if forming is not None and start == forming.start:
                forming.add(price)
                continue
            if forming is not None and start < forming.start:
                late = True
                continue
            last = self._last_closed_start(key)
            if forming is None and last is not None and start <= last:
                late = True
                continue
            if forming is not None:  # start > forming.start: the forming bar is done
                done.append(self._close(key, forming))
            self._forming[key] = _Forming(
                start, bar_end(start, tf, self.day_offset), price, price, price, price, Decimal(1)
            )
        if late:
            self.late_ticks += 1
        return done

    def on_time(self, now: datetime) -> list[Bar]:
        """Close bars whose end (+ grace) has passed without a newer tick."""
        now = ensure_utc(now)
        done = [
            self._close(key, forming)
            for key, forming in list(self._forming.items())
            if forming.end + self.grace <= now
        ]
        for bar in done:
            self._forming.pop((bar.symbol, bar.timeframe), None)
        return done

    def _close(self, key: tuple[str, Timeframe], forming: _Forming) -> Bar:
        bar = forming.to_bar(key[0], key[1], complete=True)
        self._history.setdefault(key, deque(maxlen=self._history_len)).append(bar)
        return bar
