"""Stale-feed monitor (SPEC §11): per symbol, flags a feed as STALE when it has been silent for longer than
expected *while its trading session is open*, and LIVE again on the next tick.

The expectation adapts to each symbol's tick rate: an exponentially weighted mean of inter-tick gaps
(integer ms), times ``multiplier``, clamped to ``[min_silence, max_silence]``. Quiet symbols get a long
threshold, busy ones a short one; before enough ticks are seen the threshold is ``max_silence``.
Uses the ticks' local receive time, so broker clock drift does not matter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from trader.domain.enums import FeedState
from trader.domain.events import FeedEvent
from trader.domain.models import Instrument, Tick
from trader.domain.time import ensure_utc

_MS = timedelta(milliseconds=1)


@dataclass(slots=True)
class _Feed:
    instrument: Instrument
    watched_since: datetime
    last_recv: datetime | None = None
    last_tick_ts: datetime | None = None
    ewma_gap_ms: int | None = None
    samples: int = 0
    state: FeedState = FeedState.LIVE


class StaleFeedMonitor:
    def __init__(
        self,
        broker: str,
        *,
        min_silence: timedelta = timedelta(seconds=15),
        max_silence: timedelta = timedelta(minutes=2),
        multiplier: int = 20,
        smoothing: int = 10,  # EWMA alpha = 1/smoothing
        warmup_samples: int = 20,
    ) -> None:
        if min_silence <= timedelta(0) or max_silence < min_silence:
            raise ValueError("need 0 < min_silence <= max_silence")
        if multiplier < 1 or smoothing < 1:
            raise ValueError("multiplier and smoothing must be >= 1")
        self.broker = broker
        self.min_ms = min_silence // _MS
        self.max_ms = max_silence // _MS
        self.multiplier = multiplier
        self.smoothing = smoothing
        self.warmup_samples = warmup_samples
        self._feeds: dict[str, _Feed] = {}

    def watch(self, instrument: Instrument, now: datetime) -> None:
        if instrument.symbol not in self._feeds:
            self._feeds[instrument.symbol] = _Feed(instrument, ensure_utc(now))

    def unwatch(self, symbol: str) -> None:
        self._feeds.pop(symbol, None)

    def state(self, symbol: str) -> FeedState:
        feed = self._feeds.get(symbol)
        return feed.state if feed else FeedState.LIVE

    def threshold(self, symbol: str) -> timedelta:
        feed = self._feeds.get(symbol)
        if feed is None or feed.ewma_gap_ms is None or feed.samples < self.warmup_samples:
            return self.max_ms * _MS
        return min(self.max_ms, max(self.min_ms, feed.ewma_gap_ms * self.multiplier)) * _MS

    def observe(self, tick: Tick) -> FeedEvent | None:
        """Record a tick. Returns a LIVE event if the feed was stale."""
        feed = self._feeds.get(tick.symbol)
        if feed is None:
            return None
        recv = tick.recv_ts
        if feed.last_recv is not None and recv > feed.last_recv:
            gap = (recv - feed.last_recv) // _MS
            # Gaps across a closed session (weekend) say nothing about the live tick rate.
            if gap <= self.max_ms:
                feed.ewma_gap_ms = (
                    gap
                    if feed.ewma_gap_ms is None
                    else feed.ewma_gap_ms + (gap - feed.ewma_gap_ms) // self.smoothing
                )
                feed.samples += 1
        silence = (
            (recv - feed.last_recv) // _MS if feed.last_recv is not None and recv > feed.last_recv else 0
        )
        feed.last_recv = max(recv, feed.last_recv) if feed.last_recv else recv
        feed.last_tick_ts = tick.ts
        if feed.state is FeedState.STALE:
            feed.state = FeedState.LIVE
            return FeedEvent(
                broker=self.broker,
                symbol=tick.symbol,
                state=FeedState.LIVE,
                ts=recv,
                last_tick_ts=tick.ts,
                silence_ms=silence,
            )
        return None

    def check(self, now: datetime) -> list[FeedEvent]:
        """Return STALE transitions as of ``now`` (call periodically, e.g. every second)."""
        now = ensure_utc(now)
        events: list[FeedEvent] = []
        for symbol, feed in self._feeds.items():
            if feed.state is FeedState.STALE or not feed.instrument.is_open(now):
                continue
            since = feed.last_recv or feed.watched_since
            silence_ms = (now - since) // _MS
            if silence_ms > self.threshold(symbol) // _MS:
                feed.state = FeedState.STALE
                events.append(
                    FeedEvent(
                        broker=self.broker,
                        symbol=symbol,
                        state=FeedState.STALE,
                        ts=now,
                        last_tick_ts=feed.last_tick_ts,
                        silence_ms=silence_ms,
                    )
                )
        return events
