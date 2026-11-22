from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from trader.brokers.base import RateLimit


@dataclass(frozen=True, slots=True, kw_only=True)
class PaperSettings:
    """Fill-simulation parameters. Defaults are a frictionless market; tune them to match a real account."""

    account_id: str = "paper"
    currency: str = "USD"
    starting_balance: Decimal = Decimal(10_000)
    leverage: int = 100
    hedging: bool = True  # False = netting (one position per symbol)
    slippage_points: int = 0  # adverse, in ticks, applied to market and stop fills
    extra_spread_points: int = 0  # widen the source spread by this many ticks (half each side)
    latency_ms: int = 0  # delay between ACK and execution
    commission_per_lot: Decimal = Decimal(0)  # account currency, charged per side
    swap_long_per_lot: Decimal = Decimal(0)  # account currency per lot per night (negative = cost)
    swap_short_per_lot: Decimal = Decimal(0)
    rollover_hour_utc: int = 21
    triple_swap_weekday: int = 2  # Wednesday rollover charges 3 nights
    rate_limit: RateLimit | None = None  # emulate broker-side request limits (raises RateLimited)
    account_event_interval: timedelta = timedelta(seconds=2)  # tick-time interval for AccountEvents

    def __post_init__(self) -> None:
        if self.leverage <= 0:
            raise ValueError("leverage must be > 0")
        if min(self.slippage_points, self.extra_spread_points, self.latency_ms) < 0:
            raise ValueError("slippage, spread and latency must be >= 0")
        if not 0 <= self.rollover_hour_utc <= 23:
            raise ValueError("rollover_hour_utc must be 0..23")
        if self.starting_balance < 0 or self.commission_per_lot < 0:
            raise ValueError("starting_balance and commission_per_lot must be >= 0")
