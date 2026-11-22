"""Pure P&L and margin maths used by the paper broker. Amounts are in the instrument's quote or margin
currency; callers convert to the account currency with ``trader.domain.fx``."""

from __future__ import annotations

from decimal import Decimal

from trader.domain.enums import Side
from trader.domain.models import Instrument


def pnl_quote(
    side: Side, volume: Decimal, entry: Decimal, exit_price: Decimal, contract_size: Decimal
) -> Decimal:
    """Gross P&L in the quote currency for closing ``volume`` lots opened at ``entry``."""
    return side.sign * (exit_price - entry) * volume * contract_size


def margin_required(inst: Instrument, volume: Decimal, price: Decimal, leverage: int) -> tuple[Decimal, str]:
    """Margin for ``volume`` lots, returned with its currency.

    FX (margin currency = base): units / leverage, in base currency.
    CFDs/metals (margin currency = quote): units * price / leverage, in quote currency.
    """
    units = volume * inst.contract_size
    if inst.margin_currency == inst.base_currency:
        return units / leverage, inst.base_currency
    return units * price / leverage, inst.margin_currency


def weighted_entry(vol_a: Decimal, price_a: Decimal, vol_b: Decimal, price_b: Decimal) -> Decimal:
    """Average entry when adding ``vol_b`` at ``price_b`` to ``vol_a`` at ``price_a`` (netting accounts)."""
    return (vol_a * price_a + vol_b * price_b) / (vol_a + vol_b)
