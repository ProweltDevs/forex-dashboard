"""Canonical enumerations shared by every layer."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY

    @property
    def sign(self) -> int:
        """+1 for BUY, -1 for SELL. Used in P&L maths: pnl = sign * (exit - entry) * size."""
        return 1 if self is Side.BUY else -1


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class TimeInForce(StrEnum):
    GTC = "GTC"  # good till cancelled
    GTD = "GTD"  # good till date (requires expiry)
    DAY = "DAY"
    IOC = "IOC"  # immediate or cancel
    FOK = "FOK"  # fill or kill


class OrderStatus(StrEnum):
    NEW = "NEW"
    PENDING_SUBMIT = "PENDING_SUBMIT"
    ACCEPTED = "ACCEPTED"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = frozenset({OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED})


class Timeframe(StrEnum):
    S1 = "S1"
    S5 = "S5"
    S15 = "S15"
    M1 = "M1"
    M3 = "M3"
    M5 = "M5"
    M10 = "M10"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"

    @property
    def duration(self) -> timedelta | None:
        """Fixed length of the bar, or None for calendar-based timeframes (MN1)."""
        return _DURATIONS.get(self)


_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.S1: timedelta(seconds=1),
    Timeframe.S5: timedelta(seconds=5),
    Timeframe.S15: timedelta(seconds=15),
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M3: timedelta(minutes=3),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M10: timedelta(minutes=10),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.M30: timedelta(minutes=30),
    Timeframe.H1: timedelta(hours=1),
    Timeframe.H4: timedelta(hours=4),
    Timeframe.D1: timedelta(days=1),
    Timeframe.W1: timedelta(weeks=1),
}


class ConnectionState(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    DEGRADED = "DEGRADED"


class FeedState(StrEnum):
    LIVE = "LIVE"
    STALE = "STALE"


class PositionChange(StrEnum):
    OPENED = "OPENED"
    MODIFIED = "MODIFIED"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSED = "CLOSED"


class RiskDecision(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class TradingMode(StrEnum):
    PAPER = "paper"
    DEMO = "demo"
    LIVE = "live"
