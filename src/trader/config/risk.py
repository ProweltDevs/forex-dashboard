"""Risk configuration (SPEC §7). Every pre-trade limit is set per account and can be overridden per
strategy: an override names only the fields it changes. ``None`` switches a rule off. The defaults are the
conservative baseline; do not loosen them in code (CLAUDE.md), override them in config.

Percentages are plain numbers (``3`` = 3 %). Money is in the account currency. Times are UTC.
"""

from __future__ import annotations

import re
from datetime import datetime, time
from decimal import Decimal
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trader.domain.enums import KillLevel

_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_WEEKLY = re.compile(r"^(MON|TUE|WED|THU|FRI|SAT|SUN) ([01]\d|2[0-3]):([0-5]\d)$")
_DAILY = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def parse_weekly(value: str) -> tuple[int, time]:
    """'FRI 21:00' -> (4, 21:00). Weekday follows ``datetime.weekday()`` (Mon=0)."""
    m = _WEEKLY.fullmatch(value)
    if m is None:
        raise ValueError(f"expected 'DDD HH:MM' (e.g. 'FRI 21:00'), got {value!r}")
    return _WEEKDAYS.index(m.group(1)), time(int(m.group(2)), int(m.group(3)))


def parse_daily(value: str) -> time:
    m = _DAILY.fullmatch(value)
    if m is None:
        raise ValueError(f"expected 'HH:MM', got {value!r}")
    return time(int(m.group(1)), int(m.group(2)))


class Blackout(_Model):
    """A manual no-trading window (e.g. a news release). Empty ``symbols`` and ``currencies`` = all."""

    start: datetime
    end: datetime
    symbols: tuple[str, ...] = ()
    currencies: tuple[str, ...] = ()  # matches an instrument's base or quote currency
    reason: str = "blackout"

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("blackout start/end need a timezone (use ...Z for UTC)")
        if self.end <= self.start:
            raise ValueError("blackout end must be after start")
        return self


class SessionRules(_Model):
    use_instrument_sessions: bool = True  # Instrument.sessions from the broker, when it provides them
    weekend: tuple[str, str] | None = ("FRI 21:00", "SUN 21:00")  # no entries from .. to (UTC)
    rollover: tuple[str, str] | None = ("20:55", "21:10")  # daily, UTC; wide spreads around swap time
    blackouts: tuple[Blackout, ...] = ()

    @field_validator("weekend")
    @classmethod
    def _check_weekend(cls, value: tuple[str, str] | None) -> tuple[str, str] | None:
        if value is not None:
            parse_weekly(value[0])
            parse_weekly(value[1])
        return value

    @field_validator("rollover")
    @classmethod
    def _check_rollover(cls, value: tuple[str, str] | None) -> tuple[str, str] | None:
        if value is not None:
            parse_daily(value[0])
            parse_daily(value[1])
        return value


class RiskLimits(_Model):
    """Pre-trade limits for one account, or one strategy's overrides of them."""

    # Size and count
    max_order_volume: Decimal | None = Field(default=Decimal("1"), gt=0)  # lots per order
    # lots per symbol, both sides: open positions + working orders + this order
    max_symbol_volume: Decimal | None = Field(default=Decimal("2"), gt=0)
    # positions + working entry orders (an order may become a position)
    max_positions_account: int | None = Field(default=10, ge=1)
    max_positions_symbol: int | None = Field(default=3, ge=1)
    max_positions_strategy: int | None = Field(default=5, ge=1)

    # Exposure and margin, after the trade (account currency)
    max_gross_exposure: Decimal | None = Field(default=None, gt=0)  # sum of |notional|
    max_net_exposure: Decimal | None = Field(default=None, gt=0)  # |sum of signed notional|
    max_gross_leverage: Decimal | None = Field(default=Decimal("10"), gt=0)  # gross notional / equity
    max_net_leverage: Decimal | None = Field(default=Decimal("5"), gt=0)
    max_margin_usage_pct: Decimal | None = Field(default=Decimal("30"), gt=0, le=100)  # margin / equity

    # Per trade
    require_sl: bool = True  # set false per strategy to whitelist it
    max_loss_per_trade: Decimal | None = Field(default=None, gt=0)  # money lost if the SL is hit
    max_loss_per_trade_pct: Decimal | None = Field(default=Decimal("1"), gt=0, le=100)  # of equity

    # Account loss (realized + unrealized, i.e. equity)
    daily_loss: Decimal | None = Field(default=None, gt=0)  # from equity at the trading-day start
    daily_loss_pct: Decimal | None = Field(default=Decimal("3"), gt=0, le=100)
    max_drawdown_pct: Decimal | None = Field(default=Decimal("10"), gt=0, le=100)  # from the equity peak

    # Order flow
    max_orders_per_minute: int | None = Field(default=20, ge=1)  # account-wide runaway-loop guard
    max_orders_per_minute_strategy: int | None = Field(default=10, ge=1)
    max_order_to_fill_ratio: Decimal | None = Field(default=Decimal("10"), gt=0)
    order_to_fill_min_orders: int = Field(default=20, ge=1)  # ratio checked only past this many orders
    order_to_fill_window_min: int = Field(default=60, ge=1)
    duplicate_window_s: Decimal | None = Field(default=Decimal("10"), gt=0)  # same strategy+symbol+side

    # Market conditions
    max_spread_multiple: Decimal | None = Field(default=Decimal("3"), gt=0)  # x typical spread
    max_spread_points: dict[str, int] = Field(default_factory=dict)  # symbol -> ticks (absolute cap)
    max_tick_age_s: Decimal | None = Field(default=Decimal("5"), gt=0)  # stale-price guard
    max_price_distance_pct: Decimal | None = Field(default=Decimal("2"), gt=0)  # limit/stop vs market

    sessions: SessionRules = SessionRules()


class BreakerConfig(_Model):
    """Automatic kill-switch triggers and the level each one escalates to (SPEC §7)."""

    daily_loss_level: KillLevel = KillLevel.FLATTEN_ALL  # clears at the next trading day
    drawdown_level: KillLevel = KillLevel.FLATTEN_ALL  # latched until a manual reset
    max_consecutive_rejects: int | None = Field(default=5, ge=1)  # broker rejects in a row
    rejects_level: KillLevel = KillLevel.PAUSE_NEW  # latched
    feed_stale_s: Decimal | None = Field(default=Decimal("60"), gt=0)  # a traded symbol without ticks
    feed_stale_level: KillLevel = KillLevel.PAUSE_NEW  # clears when ticks resume
    disconnected_s: Decimal | None = Field(default=Decimal("60"), gt=0)
    # PAUSE_NEW: cancels cannot reach a disconnected broker; the OMS refuses entries until reconciled anyway
    disconnected_level: KillLevel = KillLevel.PAUSE_NEW  # clears on reconnect
    reconcile_kinds: tuple[str, ...] = ("position_corrected", "order_abandoned")
    reconcile_level: KillLevel = KillLevel.PAUSE_NEW  # latched
    max_clock_drift_ms: int | None = Field(default=2000, ge=1)  # local vs broker tick timestamps
    clock_drift_level: KillLevel = KillLevel.PAUSE_NEW  # clears when the drift is back in range


class RiskConfig(_Model):
    limits: RiskLimits = RiskLimits()
    strategies: dict[str, RiskLimits] = Field(default_factory=dict)  # per-strategy overrides
    breakers: BreakerConfig = BreakerConfig()
    day_offset_minutes: int = Field(default=0, ge=-720, le=720)  # trading day start vs 00:00 UTC

    def for_strategy(self, strategy_id: str) -> RiskLimits:
        """Account limits with the strategy's overrides applied (only the fields it sets)."""
        override = self.strategies.get(strategy_id)
        if override is None:
            return self.limits
        return self.limits.model_copy(update={k: getattr(override, k) for k in override.model_fields_set})
