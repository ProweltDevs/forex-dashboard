from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.factories import eurusd
from trader.domain.models import Instrument
from trader.risk import sizing

D = Decimal
EURUSD = eurusd()
XAUUSD = eurusd(
    symbol="XAUUSD",
    digits=2,
    tick_size=D("0.01"),
    contract_size=D(100),
    max_volume=D(50),
    base_currency="XAU",
    margin_currency="USD",
)

money = st.decimals(min_value=D("0.01"), max_value=D(1_000_000), places=2)
distance = st.decimals(min_value=D("0.00001"), max_value=D("0.05"), places=5)
rates = st.decimals(min_value=D("0.005"), max_value=D(200), places=4)
instruments = st.sampled_from([EURUSD, XAUUSD])


def _valid(inst: Instrument, lots: Decimal | None) -> bool:
    return lots is None or inst.is_valid_volume(lots)


@given(instruments, money, distance, rates)
def test_fixed_risk_never_exceeds_the_risk_and_is_a_valid_volume(
    inst: Instrument, risk: Decimal, dist: Decimal, rate: Decimal
) -> None:
    entry = D("1.10000") if inst is EURUSD else D("2000.00")
    lots = sizing.fixed_money_risk(
        inst, risk_money=risk, entry=entry, stop=entry - dist, quote_to_account=rate
    )
    assert _valid(inst, lots)
    if lots is not None:
        loss = sizing.risk_per_lot(inst, entry, entry - dist, rate) * lots
        assert loss <= risk
        # Rounded down by less than one step (unless capped at the instrument max).
        if lots < inst.max_volume:
            assert sizing.risk_per_lot(inst, entry, entry - dist, rate) * (lots + inst.volume_step) > risk


@given(instruments, money, money, distance, rates)
def test_more_risk_never_gives_a_smaller_size(
    inst: Instrument, a: Decimal, b: Decimal, dist: Decimal, rate: Decimal
) -> None:
    lo, hi = sorted((a, b))
    kw = {"entry": D(100), "stop": D(100) - dist, "quote_to_account": rate}
    small = sizing.fixed_money_risk(inst, risk_money=lo, **kw)  # type: ignore[arg-type]
    big = sizing.fixed_money_risk(inst, risk_money=hi, **kw)  # type: ignore[arg-type]
    assert (small or D(0)) <= (big or D(0))


@given(st.decimals(min_value=D("-5"), max_value=D(500), places=4))
def test_cap_rounds_down_caps_and_refuses_below_min(lots: Decimal) -> None:
    capped = sizing.cap_volume(EURUSD, lots)
    if capped is None:
        assert lots < EURUSD.min_volume
    else:
        assert capped <= lots
        assert EURUSD.is_valid_volume(capped)
        assert lots - capped < EURUSD.volume_step or capped == EURUSD.max_volume


def test_known_values() -> None:
    # 1 % of 10 000 USD with a 50-pip stop on EURUSD (USD account): 100 / (0.0050 * 100 000) = 0.2 lots.
    lots = sizing.percent_of_equity(
        EURUSD, equity=D(10_000), risk_pct=D(1), entry=D("1.10000"), stop=D("1.09500"), quote_to_account=D(1)
    )
    assert lots == D("0.20")
    # ATR 0.0010 x 2 = 20 pips; 50 EUR on a EUR account (USD->EUR 0.9): 50 / (0.002*100000*0.9) = 0.277
    atr = sizing.volatility_atr(
        EURUSD, risk_money=D(50), atr=D("0.0010"), atr_multiple=D(2), quote_to_account=D("0.9")
    )
    assert atr == D("0.27")
    assert sizing.fixed_lots(EURUSD, D("0.015")) == D("0.01")
    assert sizing.fixed_lots(EURUSD, D("0.005")) is None  # never rounded UP to the minimum
    assert sizing.tick_value(EURUSD, D(1)) == D("1.00000")  # 1 USD per pip-tenth per lot
    with pytest.raises(ValueError, match="differ"):
        sizing.fixed_money_risk(EURUSD, risk_money=D(1), entry=D(1), stop=D(1), quote_to_account=D(1))
