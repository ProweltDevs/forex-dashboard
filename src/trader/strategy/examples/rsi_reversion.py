"""RSI mean reversion with a scale-out ladder: buy when RSI crosses back up through ``oversold``, sell when
it crosses back down through ``overbought``. The stop is ``atr_stop`` x ATR away; the OMS takes profit in
steps at ``scale_atr`` x ATR (``scale_fractions`` of the position at each)."""

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar

from pydantic import Field, model_validator

from trader.domain.enums import Side
from trader.domain.exits import ScaleLevel, ScaleOut
from trader.domain.models import Bar
from trader.strategy.base import Params, Strategy
from trader.strategy.context import StrategyContext
from trader.strategy.indicators import ATR, RSI

from ._common import enter, flat


class RsiParams(Params):
    rsi_period: int = Field(default=14, ge=2)
    oversold: Decimal = Field(default=Decimal(30), gt=0, lt=50)
    overbought: Decimal = Field(default=Decimal(70), gt=50, lt=100)
    atr_period: int = Field(default=14, ge=1)
    atr_stop: Decimal = Field(default=Decimal("1.5"), gt=0)
    scale_atr: tuple[Decimal, ...] = (Decimal(1), Decimal(2))
    scale_fractions: tuple[Decimal, ...] = (Decimal("0.5"), Decimal("0.5"))
    risk_pct: Decimal = Field(default=Decimal("0.5"), gt=0, le=5)

    @model_validator(mode="after")
    def _check(self) -> RsiParams:
        if len(self.scale_atr) != len(self.scale_fractions) or not self.scale_atr:
            raise ValueError("scale_atr and scale_fractions need the same, non-zero length")
        return self


class RsiReversion(Strategy[RsiParams]):
    version: ClassVar[str] = "1"
    Params: ClassVar[type[Params]] = RsiParams
    warmup_bars: ClassVar[int] = 60

    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
        p = self.params
        if bar.timeframe is not ctx.timeframes[0]:
            return
        rsi = ctx.indicator(RSI, bar.symbol, bar.timeframe, period=p.rsi_period).value
        atr = ctx.indicator(ATR, bar.symbol, bar.timeframe, period=p.atr_period).value
        if rsi is None or atr is None:
            return
        key = f"rsi:{bar.symbol}"
        prev_raw = ctx.state.get(key)
        ctx.state[key] = str(rsi)
        if not isinstance(prev_raw, str) or not flat(ctx, bar.symbol):
            return
        prev = Decimal(prev_raw)
        if prev < p.oversold <= rsi:
            side = Side.BUY
        elif prev > p.overbought >= rsi:
            side = Side.SELL
        else:
            return
        ladder = ScaleOut(
            levels=tuple(
                ScaleLevel(distance=k * atr, fraction=f)
                for k, f in zip(p.scale_atr, p.scale_fractions, strict=True)
            )
        )
        enter(ctx, bar, side, stop_distance=p.atr_stop * atr, risk_pct=p.risk_pct, exits=(ladder,))
