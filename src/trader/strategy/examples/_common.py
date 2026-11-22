"""Shared helpers for the examples."""

from __future__ import annotations

from decimal import Decimal

from trader.domain.enums import Side
from trader.domain.models import Bar
from trader.strategy.context import StrategyContext


def entry_price(ctx: StrategyContext, bar: Bar, side: Side) -> Decimal:
    """Where a market order would fill now: the live ask/bid, else the bar close."""
    tick = ctx.last_tick(bar.symbol)
    if tick is None:
        return bar.close
    return tick.ask if side is Side.BUY else tick.bid


def flat(ctx: StrategyContext, symbol: str) -> bool:
    """No position and no working order of this strategy on ``symbol``."""
    return not ctx.positions(symbol) and not ctx.orders(symbol)


def enter(
    ctx: StrategyContext, bar: Bar, side: Side, *, stop_distance: Decimal, risk_pct: Decimal, **kw: object
) -> bool:
    """Market entry sized to lose ``risk_pct`` % of equity at the stop. Returns False if it cannot size."""
    inst = ctx.instrument(bar.symbol)
    entry = entry_price(ctx, bar, side)
    stop = inst.round_price(entry - side.sign * stop_distance)
    volume = ctx.size_by_risk(bar.symbol, entry=entry, stop=stop, risk_pct=risk_pct)
    if volume is None:
        ctx.log.info("entry_skipped", symbol=bar.symbol, side=str(side), reason="size below the minimum")
        return False
    ctx.place(side, bar.symbol, volume, sl=stop, **kw)  # type: ignore[arg-type]
    return True
