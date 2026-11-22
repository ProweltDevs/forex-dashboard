"""EMA crossover with ATR stops: long when the fast EMA crosses above the slow one, short on the opposite
cross; the stop is ``atr_stop`` x ATR away and the target ``atr_target`` x ATR. An opposite cross closes the
position first."""

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar

from pydantic import Field, model_validator

from trader.domain.enums import Side
from trader.domain.models import Bar
from trader.strategy.base import Params, Strategy
from trader.strategy.context import StrategyContext
from trader.strategy.indicators import ATR, EMA

from ._common import enter


class EmaCrossParams(Params):
    fast: int = Field(default=12, ge=1)
    slow: int = Field(default=26, ge=2)
    atr_period: int = Field(default=14, ge=1)
    atr_stop: Decimal = Field(default=Decimal(2), gt=0)
    atr_target: Decimal | None = Field(default=Decimal(3), gt=0)
    risk_pct: Decimal = Field(default=Decimal("0.5"), gt=0, le=5)
    allow_short: bool = True

    @model_validator(mode="after")
    def _check(self) -> EmaCrossParams:
        if self.fast >= self.slow:
            raise ValueError("fast must be shorter than slow")
        return self


class EmaCross(Strategy[EmaCrossParams]):
    version: ClassVar[str] = "1"
    Params: ClassVar[type[Params]] = EmaCrossParams
    warmup_bars: ClassVar[int] = 100

    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
        p = self.params
        if bar.timeframe is not ctx.timeframes[0]:
            return
        fast = ctx.indicator(EMA, bar.symbol, bar.timeframe, period=p.fast).value
        slow = ctx.indicator(EMA, bar.symbol, bar.timeframe, period=p.slow).value
        atr = ctx.indicator(ATR, bar.symbol, bar.timeframe, period=p.atr_period).value
        if fast is None or slow is None or atr is None:
            return
        key = f"above:{bar.symbol}"
        above, was_above = fast > slow, ctx.state.get(key)
        ctx.state[key] = above
        if was_above is None or above == was_above:
            return  # no cross (or the first ready bar)
        side = Side.BUY if above else Side.SELL
        for pos in ctx.positions(bar.symbol):
            if pos.side is not side:
                ctx.close(pos)
        if ctx.orders(bar.symbol) or any(p_.side is side for p_ in ctx.positions(bar.symbol)):
            return
        if side is Side.SELL and not p.allow_short:
            return
        tp = None
        if p.atr_target is not None:
            tp = ctx.instrument(bar.symbol).round_price(bar.close + side.sign * p.atr_target * atr)
        enter(ctx, bar, side, stop_distance=p.atr_stop * atr, risk_pct=p.risk_pct, tp=tp)
