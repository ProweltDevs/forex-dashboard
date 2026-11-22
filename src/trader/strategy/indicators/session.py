"""Session-based indicators: VWAP and classic pivot points. A session is a trading day starting at
00:00 UTC + ``day_offset`` (-2 h = 22:00 UTC), as in the bar builder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from trader.domain.models import Bar

from .base import THREE, TWO, ZERO, Indicator


def session_of(bar: Bar, day_offset: timedelta) -> date:
    return (bar.start - day_offset).date()


class VWAP(Indicator[Decimal]):
    """Volume-weighted typical price (hlc3) since the session start. Bars carry tick volume unless the broker
    provides real volume; a session without volume yet has no value."""

    def __init__(self, day_offset: timedelta = timedelta(0)) -> None:
        self.day_offset = day_offset
        self._session: date | None = None
        self._pv = ZERO
        self._vol = ZERO

    def update(self, bar: Bar) -> None:
        session = session_of(bar, self.day_offset)
        if session != self._session:
            self._session, self._pv, self._vol, self.value = session, ZERO, ZERO, None
        self._pv += (bar.high + bar.low + bar.close) / THREE * bar.volume
        self._vol += bar.volume
        if self._vol > 0:
            self.value = self._pv / self._vol


@dataclass(frozen=True, slots=True)
class PivotLevels:
    pivot: Decimal
    r1: Decimal
    r2: Decimal
    r3: Decimal
    s1: Decimal
    s2: Decimal
    s3: Decimal


def classic_pivots(high: Decimal, low: Decimal, close: Decimal) -> PivotLevels:
    p = (high + low + close) / THREE
    return PivotLevels(
        pivot=p,
        r1=TWO * p - low,
        s1=TWO * p - high,
        r2=p + (high - low),
        s2=p - (high - low),
        r3=high + TWO * (p - low),
        s3=low - TWO * (high - p),
    )


class Pivots(Indicator[PivotLevels]):
    """Classic floor pivots from the previous session's high, low and close; available from the second
    session on and fixed for the whole session."""

    def __init__(self, day_offset: timedelta = timedelta(0)) -> None:
        self.day_offset = day_offset
        self._session: date | None = None
        self._high = self._low = self._close = ZERO

    def update(self, bar: Bar) -> None:
        session = session_of(bar, self.day_offset)
        if session != self._session:
            if self._session is not None:
                self.value = classic_pivots(self._high, self._low, self._close)
            self._session, self._high, self._low = session, bar.high, bar.low
        else:
            self._high, self._low = max(self._high, bar.high), min(self._low, bar.low)
        self._close = bar.close
