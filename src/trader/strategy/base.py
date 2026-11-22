"""Strategy base class (SPEC §8).

Strategies are deterministic and side-effect free (CLAUDE.md rule 4): they read the world through their
``StrategyContext`` and act only by returning intents/commands through it. No wall clock (``ctx.now()``),
no network or file IO, no unseeded randomness (``ctx.random``). The same code will run in the backtester.

Callbacks are synchronous and must be fast (the runner logs callbacks over the per-strategy budget). A
callback that raises is rolled back: the commands it queued are dropped and ``ctx.state`` is restored.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from trader.domain.events import OrderEvent, PositionEvent
    from trader.domain.models import Bar, Tick

    from .context import StrategyContext


class StrategyStatus(StrEnum):
    LOADED = "LOADED"
    WARMING = "WARMING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"


class Params(BaseModel):
    """Base for strategy parameter models: unknown keys are errors, values are immutable."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Strategy[P: Params]:
    """Subclass, set ``version`` and ``Params``, override the hooks you need (all are optional).

    ``id`` is the instance id from config (several instances of one class may run with different params);
    it is the ``strategy_id`` stamped on every order. Bump ``version`` whenever the logic changes: it is
    stored with every order too.
    """

    version: ClassVar[str] = "1"
    Params: ClassVar[type[Params]] = Params
    warmup_bars: ClassVar[int] = 0  # history the strategy needs per subscribed timeframe

    def __init__(self, strategy_id: str, params: P) -> None:
        self.id = strategy_id
        self.params = params

    def on_start(self, ctx: StrategyContext) -> None:
        """After warm-up and state restore; create indicators and timers here."""

    def on_tick(self, ctx: StrategyContext, tick: Tick) -> None:
        """Every tick of a subscribed symbol."""

    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
        """Every COMPLETE bar of a subscribed symbol and timeframe; indicators are already updated."""

    def on_order(self, ctx: StrategyContext, ev: OrderEvent) -> None:
        """Every change of one of this strategy's orders (fills show as filled_volume changes)."""

    def on_position(self, ctx: StrategyContext, ev: PositionEvent) -> None:
        """Every change of one of this strategy's positions."""

    def on_timer(self, ctx: StrategyContext, name: str) -> None:
        """A timer set with ``ctx.set_timer`` fired."""

    def on_stop(self, ctx: StrategyContext) -> None:
        """Before the instance stops (state is persisted right after)."""
