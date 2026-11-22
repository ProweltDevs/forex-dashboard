"""Position sizing (SPEC §7). Pure functions over Decimal; strategies call them through their context.

Every method rounds DOWN to the instrument's volume step and caps at its max volume, so the money at risk
never exceeds what was asked. A size below the instrument minimum returns ``None`` (cannot trade it), never
the minimum: rounding up would risk more than intended.

``quote_to_account`` converts the instrument's quote currency to the account currency
(``trader.domain.fx.conversion_rate(inst.quote_currency, account_ccy, ...)``).
"""

from __future__ import annotations

from decimal import Decimal

from trader.domain.models import Instrument
from trader.domain.money import floor_to_step, require_positive

ZERO = Decimal(0)


def cap_volume(inst: Instrument, lots: Decimal) -> Decimal | None:
    """Cap at max volume and round down to the step. ``None`` if the result is below the minimum."""
    if lots <= 0:
        return None
    rounded = floor_to_step(min(lots, inst.max_volume), inst.volume_step)
    return rounded if rounded >= inst.min_volume else None


def risk_per_lot(inst: Instrument, entry: Decimal, stop: Decimal, quote_to_account: Decimal) -> Decimal:
    """Account-currency loss of one lot if price moves from ``entry`` to ``stop``."""
    require_positive("quote_to_account", quote_to_account)
    return abs(entry - stop) * inst.contract_size * quote_to_account


def tick_value(inst: Instrument, quote_to_account: Decimal, lots: Decimal = Decimal(1)) -> Decimal:
    """Account-currency value of a one-tick move for ``lots``."""
    return inst.tick_size * inst.contract_size * lots * quote_to_account


def fixed_lots(inst: Instrument, lots: Decimal) -> Decimal | None:
    return cap_volume(inst, lots)


def fixed_money_risk(
    inst: Instrument, *, risk_money: Decimal, entry: Decimal, stop: Decimal, quote_to_account: Decimal
) -> Decimal | None:
    """Lots such that hitting ``stop`` loses at most ``risk_money``."""
    require_positive("risk_money", risk_money)
    per_lot = risk_per_lot(inst, entry, stop, quote_to_account)
    if per_lot <= 0:
        raise ValueError("stop must differ from entry")
    return cap_volume(inst, risk_money / per_lot)


def percent_of_equity(
    inst: Instrument,
    *,
    equity: Decimal,
    risk_pct: Decimal,
    entry: Decimal,
    stop: Decimal,
    quote_to_account: Decimal,
) -> Decimal | None:
    """Risk ``risk_pct`` % of equity on the distance to the stop."""
    require_positive("equity", equity)
    require_positive("risk_pct", risk_pct)
    return fixed_money_risk(
        inst, risk_money=equity * risk_pct / 100, entry=entry, stop=stop, quote_to_account=quote_to_account
    )


def volatility_atr(
    inst: Instrument,
    *,
    risk_money: Decimal,
    atr: Decimal,
    atr_multiple: Decimal,
    quote_to_account: Decimal,
) -> Decimal | None:
    """The stop sits ``atr_multiple`` x ATR away; size so that hitting it loses at most ``risk_money``."""
    require_positive("atr", atr)
    require_positive("atr_multiple", atr_multiple)
    return fixed_money_risk(
        inst,
        risk_money=risk_money,
        entry=atr * atr_multiple,
        stop=ZERO,
        quote_to_account=quote_to_account,
    )
