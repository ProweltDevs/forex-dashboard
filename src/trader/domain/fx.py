"""Currency conversion from live ticks. Pure: the caller supplies a last-tick lookup."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from .errors import ConversionUnavailable
from .models import Tick

type LastTick = Callable[[str], Tick | None]

ONE = Decimal(1)


def conversion_rate(from_ccy: str, to_ccy: str, last_tick: LastTick) -> Decimal:
    """Rate to multiply an amount in ``from_ccy`` by to get ``to_ccy``, using the mid of the direct pair
    (FROMTO) or the inverse of the reverse pair (TOFROM). Canonical FX symbols are base+quote."""
    if from_ccy == to_ccy:
        return ONE
    direct = last_tick(f"{from_ccy}{to_ccy}")
    if direct is not None:
        return direct.mid
    inverse = last_tick(f"{to_ccy}{from_ccy}")
    if inverse is not None:
        return ONE / inverse.mid
    raise ConversionUnavailable(f"no live tick to convert {from_ccy} -> {to_ccy}")
