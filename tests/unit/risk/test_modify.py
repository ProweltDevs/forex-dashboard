"""Changes to pending orders and to open positions' stops go through risk (CLAUDE.md: nothing bypasses
risk). Risk-reducing changes always pass, also under a kill switch."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from tests.unit.risk.test_engine import NOW, engine, fresh
from tests.unit.risk.test_rules import position, working
from trader.domain import KillLevel
from trader.domain.enums import Side
from trader.domain.models import OrderIntent, OrderRequest
from trader.risk.modify import order_change_adds_risk, stop_change_adds_risk

D = Decimal


def modified(req: OrderRequest, **changes: object) -> OrderIntent:
    fields: dict[str, object] = {
        "symbol": req.symbol,
        "side": req.side,
        "order_type": req.order_type,
        "volume": req.volume,
        "strategy_id": req.strategy_id,
        "strategy_version": req.strategy_version,
        "signal_ts": NOW,
        "limit_price": req.limit_price,
        "stop_price": req.stop_price,
        "sl": req.sl,
        "tp": req.tp,
    }
    fields.update(changes)
    return OrderIntent(**fields)  # type: ignore[arg-type]


def with_sl(sl: str | None, volume: str = "0.10") -> OrderRequest:
    req = working(volume=volume).request
    return replace(req, sl=None if sl is None else D(sl))


def test_what_adds_risk_on_an_order() -> None:
    req = with_sl("1.08500")
    assert order_change_adds_risk(req, modified(req, volume=D("0.20")))
    assert not order_change_adds_risk(req, modified(req, volume=D("0.05")))
    assert order_change_adds_risk(req, modified(req, limit_price=D("1.09100")))  # a new entry price
    assert order_change_adds_risk(req, modified(req, sl=None))  # stop removed
    assert order_change_adds_risk(req, modified(req, sl=D("1.08000")))  # moved away
    assert not order_change_adds_risk(req, modified(req, sl=D("1.08800")))  # tightened
    assert not order_change_adds_risk(req, modified(req, tp=D("1.10000")))  # TP only
    bare = with_sl(None)
    assert not order_change_adds_risk(bare, modified(bare, sl=D("1.08500")))  # a stop added


@given(
    entry=st.decimals(min_value=D("0.5"), max_value=D("2"), places=5),
    old=st.decimals(min_value=D("0.0005"), max_value=D("0.05"), places=5),
    shift=st.decimals(min_value=D("0"), max_value=D("0.04"), places=5),
    side=st.sampled_from(Side),
)
def test_tightening_never_adds_risk_and_widening_always_does(
    entry: Decimal, old: Decimal, shift: Decimal, side: Side
) -> None:
    sign = 1 if side is Side.BUY else -1
    pos = replace(position(side=side), entry_price=entry, sl=entry - sign * old)
    assert pos.sl is not None
    assert not stop_change_adds_risk(pos, pos.sl + sign * min(shift, old))  # toward or to entry
    if shift:
        assert stop_change_adds_risk(pos, pos.sl - sign * shift)
    assert stop_change_adds_risk(pos, None)


def test_order_change_rules() -> None:
    eng, clock = engine(limits={"max_symbol_volume": "0.2", "max_orders_per_minute_strategy": 1})
    # A stop 5 pips under the 1.09000 limit: small losses, low leverage; only the rule under test bites.
    order = replace(working(volume="0.10"), request=with_sl("1.08950", volume="0.10"))
    v = fresh(clock, open_orders=(order,))
    req = order.request

    big = eng.pre_modify(req, modified(req, volume=D("5")), v)
    assert (big.approved, big.rule) == (False, "max_order_volume")
    # The order's own volume is replaced, not added: 0.18 alone fits the 0.2 cap (0.28 would not).
    assert eng.pre_modify(req, modified(req, volume=D("0.18")), v).approved
    over = eng.pre_modify(req, modified(req, volume=D("0.25")), v)
    assert (over.approved, over.rule) == (False, "max_symbol_volume")
    unprotected = eng.pre_modify(req, modified(req, sl=None), v)
    assert (unprotected.approved, unprotected.rule) == (False, "loss_per_trade")
    # Modifications are not new orders: the per-minute cap (1) still lets a new order through.
    assert eng.pre_modify(req, modified(req, volume=D("0.15")), v).approved
    assert eng.pre_trade(modified(req, volume=D("0.10"), strategy_id="ema"), v).approved


def test_kill_switch_blocks_risk_adding_changes_only() -> None:
    eng, clock = engine()
    order = replace(working(), request=with_sl("1.08500"))
    v = fresh(clock, open_orders=(order,))
    eng.kill(KillLevel.PAUSE_NEW, "operator", v)
    req = order.request
    moved = eng.pre_modify(req, modified(req, limit_price=D("1.09200")), v)
    assert (moved.approved, moved.rule) == (False, "kill_switch")
    assert eng.pre_modify(req, modified(req, volume=D("0.05")), v).approved  # reducing is always fine
    assert eng.pre_modify(req, modified(req, sl=D("1.08800")), v).approved

    pos = replace(position(), sl=D("1.09500"))
    assert eng.pre_modify_position(pos, D("1.09800"), v).approved  # trailing under a kill switch: fine
    removed = eng.pre_modify_position(pos, None, v)
    assert (removed.approved, removed.rule) == (False, "kill_switch")


def test_position_stop_rules() -> None:
    eng, clock = engine()  # require_sl, max loss 1 % of 10 000 = 100 USD
    v = fresh(clock)
    pos = replace(position(volume="0.10"), sl=D("1.09500"))
    removed = eng.pre_modify_position(pos, None, v)
    assert (removed.approved, removed.rule) == (False, "loss_per_trade")
    assert "SL is required" in removed.reason
    # 0.10 lot from the 1.10010 ask: a stop at 1.08000 risks 201 USD > 100.
    wide = eng.pre_modify_position(pos, D("1.08000"), v)
    assert (wide.approved, wide.rule) == (False, "loss_per_trade")
    assert eng.pre_modify_position(pos, D("1.09400"), v).approved  # wider, but 61 USD: within the cap
    assert eng.pre_modify_position(pos, D("1.09800"), v).approved  # tighter
    bare = replace(position(), sl=None)
    assert eng.pre_modify_position(bare, D("1.09000"), v).approved  # adding a stop
    loose, _ = engine(strategies={"other": {"require_sl": False}})
    assert loose.pre_modify_position(pos, None, v).approved  # whitelisted strategy
