"""Donchian breakout with a time exit: buy a close above the highest high of the previous ``period`` bars,
sell a close below the lowest low; the stop is ``atr_stop`` x ATR away and the OMS closes the position
after ``exit_after_bars`` bars if neither the stop nor the target hit first."""

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar

from pydantic import Field

from trader.domain.enums import Side
from trader.domain.exits import TimeExit
from trader.domain.models import Bar
from trader.strategy.base import Params, Strategy
from trader.strategy.context import StrategyContext
from trader.strategy.indicators import ATR, Donchian

from ._common import enter, flat


class DonchianParams(Params):
    period: int = Field(default=20, ge=2)
    atr_period: int = Field(default=14, ge=1)
    atr_stop: Decimal = Field(default=Decimal(2), gt=0)
    exit_after_bars: int = Field(default=24, ge=1)
    risk_pct: Decimal = Field(default=Decimal("0.5"), gt=0, le=5)


class DonchianBreakout(Strategy[DonchianParams]):
    version: ClassVar[str] = "1"
    Params: ClassVar[type[Params]] = DonchianParams
    warmup_bars: ClassVar[int] = 60

    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
        p = self.params
        if bar.timeframe is not ctx.timeframes[0]:
            return
        channel = ctx.indicator(
            Donchian, bar.symbol, bar.timeframe, period=p.period, exclude_current=True
        ).value
        atr = ctx.indicator(ATR, bar.symbol, bar.timeframe, period=p.atr_period).value
        if channel is None or atr is None or not flat(ctx, bar.symbol):
            return
        if bar.close > channel.upper:
            side = Side.BUY
        elif bar.close < channel.lower:
            side = Side.SELL
        else:
            return
        duration = bar.timeframe.duration
        exits = (TimeExit(after=duration * p.exit_after_bars),) if duration is not None else ()
        enter(ctx, bar, side, stop_distance=p.atr_stop * atr, risk_pct=p.risk_pct, exits=exits)
