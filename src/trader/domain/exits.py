"""Exit instructions a strategy attaches to an entry order. The OMS turns them into emulated orders
(SPEC §6) that run on live ticks and survive restarts. All distances are in price units."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from .money import require_positive


@dataclass(frozen=True, slots=True, kw_only=True)
class TrailingStop:
    """Keep the SL ``distance`` behind the best price once the trade is ``activation`` in profit
    (0 = immediately). The stop only moves in the trade's favour, in steps of at least ``step``."""

    distance: Decimal
    activation: Decimal = Decimal(0)
    step: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        require_positive("distance", self.distance)
        if self.activation < 0 or self.step < 0:
            raise ValueError("activation and step must be >= 0")


@dataclass(frozen=True, slots=True, kw_only=True)
class BreakEven:
    """Once the trade is ``trigger`` in profit, move the SL to entry + ``offset`` (in the trade's favour)."""

    trigger: Decimal
    offset: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        require_positive("trigger", self.trigger)
        if self.offset < 0 or self.offset >= self.trigger:
            raise ValueError("need 0 <= offset < trigger")


@dataclass(frozen=True, slots=True, kw_only=True)
class TimeExit:
    """Close the position ``after`` this long (bars: pass N x timeframe duration)."""

    after: timedelta

    def __post_init__(self) -> None:
        if self.after <= timedelta(0):
            raise ValueError("after must be > 0")


@dataclass(frozen=True, slots=True, kw_only=True)
class ScaleLevel:
    """Close ``fraction`` of the *initial* volume when the trade is ``distance`` in profit."""

    distance: Decimal
    fraction: Decimal

    def __post_init__(self) -> None:
        require_positive("distance", self.distance)
        if not Decimal(0) < self.fraction <= 1:
            raise ValueError("fraction must be in (0, 1]")


@dataclass(frozen=True, slots=True, kw_only=True)
class ScaleOut:
    """TP1/TP2/TP3 ladder. Volumes are rounded down to the instrument step; the last level closes the rest."""

    levels: tuple[ScaleLevel, ...]

    def __post_init__(self) -> None:
        if not self.levels:
            raise ValueError("at least one level")
        distances = [lv.distance for lv in self.levels]
        if distances != sorted(distances) or len(set(distances)) != len(distances):
            raise ValueError("levels must have strictly increasing distances")
        if sum((lv.fraction for lv in self.levels), Decimal(0)) > 1:
            raise ValueError("fractions must sum to <= 1")


type ExitSpec = TrailingStop | BreakEven | TimeExit | ScaleOut
