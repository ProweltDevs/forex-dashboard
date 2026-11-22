"""Every pre-trade rule has an approve and a reject test (phase 6 acceptance).

``base()`` is an intent that passes every rule. Each case changes one thing and asserts that exactly the
rule under test rejects it, and that the same rule approves the baseline.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal

import pytest

from tests.factories import eurusd, intent
from trader.config.risk import Blackout, RiskConfig, RiskLimits, SessionRules
from trader.domain import OrderStatus, OrderType, Position, Side, Tick
from trader.domain.models import AccountState, Order, StatusChange, TradingSession
from trader.risk.gate import AccountView
from trader.risk.rules import RULES, FlowStats, RuleInput

D = Decimal
NOW = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)  # a Tuesday, mid-session
EURUSD = eurusd()
USDJPY = eurusd(
    symbol="USDJPY",
    broker_symbol="USDJPY",
    digits=3,
    tick_size=D("0.001"),
    base_currency="USD",
    quote_currency="JPY",
    margin_currency="USD",
)
EURGBP = eurusd(symbol="EURGBP", broker_symbol="EURGBP", quote_currency="GBP")


def tick(symbol: str, bid: str, spread: str = "0.00010", age_s: float = 1) -> Tick:
    b = D(bid)
    ts = NOW - timedelta(seconds=age_s)
    return Tick(symbol=symbol, bid=b, ask=b + D(spread), ts=ts, recv_ts=ts, recv_mono_ns=0)


def account(equity: str = "10000", margin: str = "0") -> AccountState:
    e, m = D(equity), D(margin)
    return AccountState(
        account_id="a", currency="USD", balance=e, equity=e, margin=m, free_margin=e - m, leverage=100, ts=NOW
    )


def position(
    symbol: str = "EURUSD", volume: str = "0.10", strategy: str = "other", side: Side = Side.BUY
) -> Position:
    return Position(
        position_id=f"P{symbol}{volume}{strategy}{side}",
        symbol=symbol,
        side=side,
        volume=D(volume),
        entry_price=D("1.10000"),
        opened_ts=NOW - timedelta(hours=1),
        strategy_id=strategy,
    )


def working(symbol: str = "EURUSD", volume: str = "0.10", strategy: str = "other") -> Order:
    req = intent(
        symbol=symbol,
        volume=D(volume),
        strategy_id=strategy,
        order_type=OrderType.LIMIT,
        limit_price=D("1.09000"),
    ).to_request(f"C{symbol}{volume}{strategy}")
    return Order(
        request=req, status=OrderStatus.WORKING, history=(StatusChange(status=OrderStatus.WORKING, ts=NOW),)
    )


def view(**kw: object) -> AccountView:
    fields: dict[str, object] = {
        "account_id": "a",
        "account": account(),
        "positions": (),
        "open_orders": (),
        "instruments": {"EURUSD": EURUSD, "USDJPY": USDJPY, "EURGBP": EURGBP},
        "last_ticks": {"EURUSD": tick("EURUSD", "1.10000"), "USDJPY": tick("USDJPY", "150.000", "0.010")},
        "broker_connected": True,
    }
    fields.update(kw)
    return AccountView(**fields)  # type: ignore[arg-type]


def flow(**kw: object) -> FlowStats:
    fields: dict[str, object] = {
        "orders_last_minute": 0,
        "strategy_orders_last_minute": 0,
        "window_orders": 0,
        "window_fills": 0,
        "last_same_signal": None,
    }
    fields.update(kw)
    return FlowStats(**fields)  # type: ignore[arg-type]


def base(**kw: object) -> RuleInput:
    """BUY 0.10 EURUSD at market (ask 1.10010), SL 50 pips below: loss 51 USD < 1 % of 10 000."""
    fields: dict[str, object] = {
        "intent": intent(strategy_id="ema", sl=D("1.09500"), tp=D("1.11000")),
        "view": view(),
        "limits": RiskLimits(),
        "now": NOW,
        "flow": flow(),
        "typical_spread": D("0.00010"),
        "day_start_equity": D(10_000),
        "peak_equity": D(10_000),
    }
    fields.update(kw)
    return RuleInput(**fields)  # type: ignore[arg-type]


def with_intent(**kw: object) -> RuleInput:
    return base(intent=replace(base().intent, **kw))


def with_limits(r: RuleInput | None = None, **kw: object) -> RuleInput:
    r = r or base()
    return replace(r, limits=r.limits.model_copy(update=kw))


def rejected_by(r: RuleInput) -> list[str]:
    return [name for name, rule in RULES.items() if rule(r) is not None]


def test_baseline_passes_every_rule() -> None:
    assert rejected_by(base()) == []


Case = tuple[str, Callable[[], RuleInput], str]

REJECT: list[Case] = [
    ("instrument", lambda: with_intent(symbol="GBPUSD"), "unknown instrument"),
    ("instrument", lambda: with_intent(volume=D("0.015")), "not a valid size"),
    ("broker_connected", lambda: base(view=view(broker_connected=False)), "not connected"),
    ("max_order_volume", lambda: with_intent(volume=D("1.50"), sl=D("1.09990")), "> 1 lots"),
    (
        "max_symbol_volume",
        lambda: base(view=view(positions=(position(volume="1.50"),), open_orders=(working(volume="0.45"),))),
        "> 2",
    ),
    (
        "max_positions_account",
        lambda: base(
            view=view(
                positions=tuple(position(volume=f"0.0{i}", symbol="USDJPY") for i in range(1, 10)),
                open_orders=(working(symbol="EURGBP"),),
            )
        ),
        "max 10",
    ),
    (
        "max_positions_symbol",
        lambda: base(
            view=view(
                positions=(position(volume="0.01"), position(volume="0.02")),
                open_orders=(working(volume="0.01"),),
            )
        ),
        "max 3",
    ),
    (
        "max_positions_strategy",
        lambda: base(view=view(positions=tuple(position("USDJPY", f"0.0{i}", "ema") for i in range(1, 6)))),
        "max 5",
    ),
    ("session", lambda: base(now=datetime(2026, 1, 10, 12, 0, tzinfo=UTC)), "weekend"),  # Saturday
    ("session", lambda: base(now=NOW.replace(hour=21, minute=0)), "rollover"),
    (
        "session",
        lambda: with_limits(
            sessions=SessionRules(
                blackouts=(
                    Blackout(
                        start=NOW - timedelta(minutes=5),
                        end=NOW + timedelta(minutes=5),
                        currencies=("USD",),
                        reason="NFP",
                    ),
                )
            )
        ),
        "NFP",
    ),
    (
        "session",
        lambda: base(
            view=view(
                instruments={
                    "EURUSD": eurusd(
                        sessions=(TradingSession(weekday=1, start=time(12, 0), end=time(20, 0)),)
                    )
                }
            )
        ),
        "outside its trading sessions",
    ),
    (
        "stale_price",
        lambda: base(view=view(last_ticks={"EURUSD": tick("EURUSD", "1.10000", age_s=10)})),
        "old",
    ),
    ("stale_price", lambda: base(view=view(last_ticks={})), "no price"),
    (
        "spread",
        lambda: base(view=view(last_ticks={"EURUSD": tick("EURUSD", "1.10000", "0.00040")})),
        "typical",
    ),
    ("spread", lambda: with_limits(max_spread_points={"EURUSD": 5}), "spread 10.0 points > 5"),
    ("price_sanity", lambda: with_intent(sl=D("1.10100")), "SL 1.10100 is on the wrong side"),
    ("price_sanity", lambda: with_intent(tp=D("1.09000")), "TP 1.09000 is on the wrong side"),
    ("price_sanity", lambda: with_intent(order_type=OrderType.STOP, stop_price=D("1.09990")), "stop"),
    (
        "price_sanity",
        lambda: with_intent(
            order_type=OrderType.LIMIT, limit_price=D("1.03000"), sl=D("1.02500"), tp=D("1.04000")
        ),
        "from the market",
    ),
    ("loss_per_trade", lambda: with_intent(sl=None), "an SL is required"),
    ("loss_per_trade", lambda: with_intent(sl=D("1.08900")), "loss at SL 111.00 USD > 100.00"),
    ("loss_per_trade", lambda: with_limits(max_loss_per_trade=D(40)), "> 40.00"),
    ("exposure", lambda: with_limits(max_gross_leverage=D("1")), "gross leverage 1.10x > 1x"),
    ("exposure", lambda: base(view=view(positions=(position(volume="0.46"),))), "net leverage 6.16x > 5x"),
    ("exposure", lambda: with_limits(max_gross_exposure=D(5000)), "gross exposure 11001.00 > 5000"),
    ("exposure", lambda: with_limits(max_net_exposure=D(5000)), "net exposure"),
    (
        "margin",
        lambda: base(view=view(account=account(margin="2950"))),
        "margin usage after the trade 30.60%",
    ),
    ("daily_loss", lambda: base(view=view(account=account(equity="9700"))), "daily loss 300.00 USD >= limit"),
    (
        "daily_loss",
        lambda: with_limits(daily_loss=D(50), r=base(view=view(account=account(equity="9950")))),
        ">= limit 50.00",
    ),
    ("drawdown", lambda: base(peak_equity=D(11_200)), "drawdown 10.71%"),
    ("orders_per_minute", lambda: base(flow=flow(orders_last_minute=20)), "max 20"),
    ("orders_per_minute", lambda: base(flow=flow(strategy_orders_last_minute=10)), "from ema"),
    ("order_to_fill", lambda: base(flow=flow(window_orders=25, window_fills=2)), "25 orders for 2 fills"),
    ("order_to_fill", lambda: base(flow=flow(window_orders=20, window_fills=0)), "0 fills"),
    ("duplicate_signal", lambda: base(flow=flow(last_same_signal=NOW - timedelta(seconds=5))), "5.0s ago"),
]


@pytest.mark.parametrize(
    ("rule", "make", "reason"), REJECT, ids=[f"{c[0]}-{i}" for i, c in enumerate(REJECT)]
)
def test_rule_rejects(rule: str, make: Callable[[], RuleInput], reason: str) -> None:
    r = make()
    why = RULES[rule](r)
    assert why is not None, f"{rule} should reject"
    assert reason in why
    assert RULES[rule](base()) is None  # and approves the baseline


APPROVE: list[Case] = [
    ("instrument", lambda: with_intent(volume=D("0.01")), "min volume"),
    (
        "max_order_volume",
        lambda: with_limits(max_order_volume=None, r=with_intent(volume=D("5"))),
        "rule off",
    ),
    (
        "max_symbol_volume",
        lambda: base(view=view(positions=(position(volume="1.90"),))),
        "exactly at the cap",
    ),
    (
        "max_positions_symbol",
        lambda: base(
            view=view(positions=(position("USDJPY"), position("USDJPY", "0.2"), position("USDJPY", "0.3")))
        ),
        "other symbol",
    ),
    ("session", lambda: base(now=datetime(2026, 1, 11, 21, 30, tzinfo=UTC)), "Sunday after the open"),
    (
        "session",
        lambda: with_limits(
            sessions=SessionRules(weekend=None), r=base(now=datetime(2026, 1, 10, 12, 0, tzinfo=UTC))
        ),
        "weekend rule off",
    ),
    (
        "session",
        lambda: with_limits(
            sessions=SessionRules(
                blackouts=(
                    Blackout(
                        start=NOW - timedelta(minutes=5), end=NOW + timedelta(minutes=5), symbols=("XAUUSD",)
                    ),
                )
            )
        ),
        "blackout for another symbol",
    ),
    ("spread", lambda: base(typical_spread=None), "no typical spread yet: absolute cap only"),
    ("price_sanity", lambda: with_intent(side=Side.SELL, sl=D("1.10500"), tp=D("1.09000")), "sell side"),
    (
        "price_sanity",
        lambda: with_intent(
            order_type=OrderType.STOP, stop_price=D("1.10100"), sl=D("1.09600"), tp=D("1.11000")
        ),
        "buy stop above the ask",
    ),
    ("loss_per_trade", lambda: with_limits(require_sl=False, r=with_intent(sl=None)), "whitelisted strategy"),
    (
        "loss_per_trade",
        lambda: base(intent=intent(symbol="USDJPY", strategy_id="ema", sl=D("149.500"))),
        "JPY loss converted with the USDJPY tick: 0.1 lot x 0.51 x 100000 / 150.005 = 34 USD",
    ),
    (
        "exposure",
        lambda: base(view=view(positions=(position(volume="0.46", side=Side.SELL),))),
        "hedged: net small",
    ),
    ("margin", lambda: base(view=view(account=account(margin="2800"))), "29.1 %"),
    ("daily_loss", lambda: base(view=view(account=account(equity="9750"))), "2.5 % down"),
    ("drawdown", lambda: base(peak_equity=D(11_000)), "9.09 % below the peak"),
    ("order_to_fill", lambda: base(flow=flow(window_orders=19, window_fills=0)), "below min orders"),
    (
        "duplicate_signal",
        lambda: base(flow=flow(last_same_signal=NOW - timedelta(seconds=11))),
        "outside window",
    ),
]


@pytest.mark.parametrize(("rule", "make", "why"), APPROVE, ids=[f"{c[0]}-{i}" for i, c in enumerate(APPROVE)])
def test_rule_approves(rule: str, make: Callable[[], RuleInput], why: str) -> None:
    assert RULES[rule](make()) is None, why


def test_every_rule_has_a_reject_and_an_approve_case() -> None:
    assert {c[0] for c in REJECT} == set(RULES)
    assert {c[0] for c in APPROVE} | {
        "broker_connected",
        "max_positions_account",
        "max_positions_strategy",
        "stale_price",
        "orders_per_minute",
    } >= set(RULES)


def test_money_rules_fail_closed_without_conversion_or_account() -> None:
    gbp = base(
        intent=intent(symbol="EURGBP", strategy_id="ema", sl=D("1.09500")),
        view=view(last_ticks={"EURGBP": tick("EURGBP", "1.10000")}),
    )
    assert "cannot value" in (RULES["loss_per_trade"](gbp) or "")  # no GBPUSD/USDGBP tick
    assert "cannot value" in (RULES["exposure"](gbp) or "")
    no_account = base(view=view(account=None))
    for rule in ("loss_per_trade", "exposure", "margin", "daily_loss", "drawdown"):
        assert RULES[rule](no_account) is not None, rule


def test_strategy_overrides_only_change_what_they_set() -> None:
    cfg = RiskConfig.model_validate(
        {
            "limits": {"max_order_volume": "2"},
            "strategies": {"scalper": {"max_order_volume": "0.2", "require_sl": False}},
        }
    )
    scalper, other = cfg.for_strategy("scalper"), cfg.for_strategy("ema")
    assert (scalper.max_order_volume, scalper.require_sl, scalper.max_symbol_volume) == (
        D("0.2"),
        False,
        D(2),
    )
    assert (other.max_order_volume, other.require_sl) == (D(2), True)
    r = base(
        intent=replace(base().intent, strategy_id="scalper", volume=D("0.30"), sl=D("1.09800")),
        limits=scalper,
    )
    assert rejected_by(r) == ["max_order_volume"]
