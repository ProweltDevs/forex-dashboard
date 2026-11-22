"""Compare bars we built from ticks with the broker's candles for the same window (phase 4 acceptance).

Why they can differ: the broker builds candles from every tick, while our stream can be conflated or
throttled (Exness documents up to one tick per 500 ms). So we can miss an extreme or the first/last tick of
a bar, but bars built from the same price side can never go *beyond* the broker's range.

Tolerance rule (see docs/adr/0006-market-data.md):
- bars are matched by exact start time (a mismatch means misaligned boundaries or day offset);
- a matched bar is "within tolerance" when |ΔO|, |ΔH|, |ΔL|, |ΔC| <= ``tolerance_points``
  (1 point = the instrument tick size);
- built high above the broker high, or built low below the broker low, by more than ``outside_points`` is a
  hard failure (wrong price side or wrong instrument);
- the window's first and last bars are excluded (partial coverage).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from trader.domain.models import Bar


@dataclass(frozen=True, slots=True)
class BarDiff:
    start: datetime
    open_pts: Decimal
    high_pts: Decimal
    low_pts: Decimal
    close_pts: Decimal

    @property
    def max_pts(self) -> Decimal:
        return max(abs(self.open_pts), abs(self.high_pts), abs(self.low_pts), abs(self.close_pts))


@dataclass(frozen=True, slots=True)
class BarComparison:
    matched: tuple[BarDiff, ...]
    missing: tuple[datetime, ...]  # broker has a bar we did not build
    extra: tuple[datetime, ...]  # we built a bar the broker does not have
    outside: tuple[datetime, ...]  # our high/low beyond the broker range: hard failure
    tolerance_points: Decimal

    @property
    def within(self) -> int:
        return sum(1 for d in self.matched if d.max_pts <= self.tolerance_points)

    @property
    def ratio(self) -> Decimal:
        return Decimal(self.within) / Decimal(len(self.matched)) if self.matched else Decimal(0)

    def passes(self, *, min_ratio: Decimal = Decimal("0.9"), max_missing: int = 1) -> bool:
        return (
            not self.outside
            and not self.extra
            and len(self.missing) <= max_missing
            and bool(self.matched)
            and self.ratio >= min_ratio
        )

    def summary(self) -> str:
        worst = max((d.max_pts for d in self.matched), default=Decimal(0))
        return (
            f"matched={len(self.matched)} within±{self.tolerance_points}pts={self.within} ({self.ratio:.0%}) "
            f"worst={worst}pts missing={len(self.missing)} extra={len(self.extra)} "
            f"outside={len(self.outside)}"
        )


def compare_bars(
    built: Sequence[Bar],
    broker: Sequence[Bar],
    *,
    tick_size: Decimal,
    tolerance_points: Decimal = Decimal(3),
    outside_points: Decimal = Decimal(1),
    trim_edges: bool = True,
) -> BarComparison:
    ours = {b.start: b for b in built if b.complete}
    theirs = {b.start: b for b in broker}
    starts = sorted(set(ours) | set(theirs))
    if trim_edges and len(starts) > 2:
        starts = starts[1:-1]
    matched: list[BarDiff] = []
    missing: list[datetime] = []
    extra: list[datetime] = []
    outside: list[datetime] = []
    for start in starts:
        a, b = ours.get(start), theirs.get(start)
        if a is None:
            missing.append(start)
            continue
        if b is None:
            extra.append(start)
            continue
        diff = BarDiff(
            start=start,
            open_pts=(a.open - b.open) / tick_size,
            high_pts=(a.high - b.high) / tick_size,
            low_pts=(a.low - b.low) / tick_size,
            close_pts=(a.close - b.close) / tick_size,
        )
        if diff.high_pts > outside_points or -diff.low_pts > outside_points:
            outside.append(start)
        matched.append(diff)
    return BarComparison(tuple(matched), tuple(missing), tuple(extra), tuple(outside), tolerance_points)


def infer_day_offset(daily_starts: Sequence[datetime]) -> timedelta:
    """The broker's trading-day alignment from D1 candle start times, as ``MarketDataConfig.day_offset``:
    the most common offset from UTC midnight, normalised to [-12 h, +12 h)."""
    if not daily_starts:
        raise ValueError("need at least one D1 start time")
    offsets = []
    for start in daily_starts:
        off = start - start.replace(hour=0, minute=0, second=0, microsecond=0)
        if off >= timedelta(hours=12):
            off -= timedelta(days=1)
        offsets.append(off)
    return Counter(offsets).most_common(1)[0][0]
