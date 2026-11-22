"""Pure P&L, margin and exposure maths shared by the paper broker and the risk engine. Amounts are in the
instrument's quote or margin currency; callers convert to the account currency with ``trader.domain.fx``."""

from __future__ import annotations

from decimal import Decimal

from .enums import Side
from .models import Instrument


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


def notional_quote(inst: Instrument, volume: Decimal, price: Decimal) -> Decimal:
    """Position value in the quote currency: units x price (always >= 0)."""
    return abs(volume) * inst.contract_size * price


def loss_at_stop(side: Side, volume: Decimal, entry: Decimal, stop: Decimal, inst: Instrument) -> Decimal:
    """Loss in the quote currency if a position opened at ``entry`` is closed at ``stop``. Zero when the
    stop is at or beyond break-even (a protective stop in profit loses nothing)."""
    return max(Decimal(0), -pnl_quote(side, volume, entry, stop, inst.contract_size))
