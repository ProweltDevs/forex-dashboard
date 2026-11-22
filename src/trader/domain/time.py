"""Time helpers. The domain uses timezone-aware UTC datetimes; the wire and storage use int epoch-ms."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class Clock(Protocol):
    """Source of the current time. Strategies get one through ``ctx.now()``; tests and the
    future backtester inject a deterministic one."""

    def now(self) -> datetime: ...


class SystemClock:
    """Wall-clock UTC time. Never used inside strategies."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class FixedClock:
    """Manually advanced clock for tests and simulation."""

    __slots__ = ("_now",)

    def __init__(self, start: datetime) -> None:
        self._now = ensure_utc(start)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        self._now += delta
        return self._now

    def set(self, value: datetime) -> None:
        self._now = ensure_utc(value)


def utc_now() -> datetime:
    """Current wall-clock time in UTC (infrastructure code only, never strategies)."""
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` converted to UTC. Naive datetimes are a bug and are rejected."""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"naive datetime is not allowed: {value!r}")
    return value.astimezone(UTC)


def to_epoch_ms(value: datetime) -> int:
    """Aware datetime -> integer milliseconds since the Unix epoch (truncates sub-ms)."""
    delta = ensure_utc(value) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def from_epoch_ms(ms: int) -> datetime:
    """Integer milliseconds since the Unix epoch -> aware UTC datetime (exact, no float maths)."""
    return _EPOCH + timedelta(milliseconds=ms)
