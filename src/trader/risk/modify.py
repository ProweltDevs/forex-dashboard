"""Does a change to a pending order or to an open position's stop add risk? (CLAUDE.md: nothing bypasses
risk.) Changes that only reduce risk are always allowed, also under a kill switch: tightening a stop,
trailing, break-even, cutting volume, adding a stop. Anything else is checked like a new order."""

from __future__ import annotations

from decimal import Decimal

from trader.domain.enums import Side
from trader.domain.models import OrderIntent, OrderRequest, Position


def stop_farther(side: Side, new: Decimal, old: Decimal) -> bool:
    """``new`` is further from the market than ``old`` (a bigger loss when hit)."""
    return new < old if side is Side.BUY else new > old


def order_change_adds_risk(current: OrderRequest, modified: OrderIntent) -> bool:
    if modified.volume > current.volume:
        return True
    if modified.limit_price != current.limit_price or modified.stop_price != current.stop_price:
        return True  # a new entry price is a new entry decision
    if current.sl is not None:
        if modified.sl is None:
            return True
        if stop_farther(current.side, modified.sl, current.sl):
            return True
    return False


def stop_change_adds_risk(position: Position, sl: Decimal | None) -> bool:
    old = position.sl
    if sl is None:
        return old is not None  # removing a stop
    if old is None:
        return False  # adding a stop
    return stop_farther(position.side, sl, old)
