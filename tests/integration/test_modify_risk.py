"""Modifications through the OMS on the paper broker are risk-checked: a refused change never reaches the
broker and is published as a RiskEvent; risk-reducing changes go through even under a kill switch."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.integration.oms_helpers import buy, make_env
from tests.integration.test_risk_oms import risk
from trader.brokers.base import OrderChanges
from trader.domain import KillLevel, OrderStatus, OrderType, RiskDecision, RiskEvent
from trader.domain.errors import OrderRejected

D = Decimal


async def test_modifications_go_through_risk() -> None:
    env = await make_env(risk=risk())
    try:
        resting = await env.oms.submit(
            buy("0.10", sl=D("1.09000"), order_type=OrderType.LIMIT, limit_price=D("1.09500"))
        )
        cid = resting.client_order_id
        await env.quote("1.10000")
        assert env.oms.order(cid).status is OrderStatus.WORKING  # type: ignore[union-attr]

        with pytest.raises(OrderRejected, match="risk:max_order_volume"):
            await env.oms.modify_order(cid, OrderChanges(volume=D("5")))
        with pytest.raises(OrderRejected, match="risk:loss_per_trade"):
            await env.oms.modify_order(cid, OrderChanges(remove_sl=True))
        [paper_order] = await env.paper.open_orders()
        assert paper_order.request.volume == D("0.10")  # nothing reached the broker
        assert paper_order.request.sl == D("1.09000")

        entry = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        await env.quote("1.10000")
        [pos] = [p for p in await env.paper.positions() if p.volume == D("0.10")]
        with pytest.raises(OrderRejected, match="risk:loss_per_trade"):
            await env.oms.modify_position(pos.position_id, None, None)  # remove the stop

        await env.oms.kill_switch(KillLevel.PAUSE_NEW, "operator")
        with pytest.raises(OrderRejected, match="risk:kill_switch"):
            await env.oms.modify_order(cid, OrderChanges(limit_price=D("1.09700")))
        await env.oms.modify_position(pos.position_id, D("1.09800"), None)  # tightening: allowed
        await env.quote("1.10000")
        [moved] = [p for p in await env.paper.positions() if p.position_id == pos.position_id]
        assert moved.sl == D("1.09800")

        refused = [e for e in env.rec.of(RiskEvent) if e.decision is RiskDecision.REJECTED]
        assert [e.rule for e in refused] == [
            "max_order_volume",
            "loss_per_trade",
            "loss_per_trade",
            "kill_switch",
        ]
        assert all(e.reason.startswith("modify") for e in refused)
        del entry
    finally:
        await env.close()


async def test_fills_past_the_price_protection_raise_an_alert() -> None:
    """Exness ignores a slippage cap on market-execution instruments, so a triggered stop-limit sent as MARKET
    (or a capped MARKET order) can fill beyond its limit. It cannot be undone; it must be seen."""
    from datetime import UTC, datetime

    from trader.domain import AlertEvent, Side
    from trader.domain.models import Fill

    env = await make_env()
    try:
        stop_limit = buy(order_type=OrderType.STOP_LIMIT, stop_price=D("1.10000"), limit_price=D("1.10030"))
        req = stop_limit.to_request("CID-STOPLIMIT-1")
        market = buy(max_slippage=D("0.00010"), signal_price=D("1.10000")).to_request("CID-MARKET-01")

        def fill(price: str, side: Side = Side.BUY) -> Fill:
            return Fill(
                fill_id=f"F{price}{side}", client_order_id=None, broker_order_id="B", position_id="P",
                symbol="EURUSD", side=side, volume=D("0.10"), price=D(price), ts=datetime.now(UTC),
            )  # fmt: skip

        env.oms._check_slippage(req, fill("1.10030"))  # at the limit: fine
        env.oms._check_slippage(req, fill("1.10045", Side.SELL))  # the closing side: not this order's entry
        env.oms._check_slippage(market, fill("1.10009"))  # within max_slippage of the signal price
        env.oms._check_slippage(req, fill("1.10045"))  # 1.5 pips past the limit
        env.oms._check_slippage(market, fill("1.10025"))  # 1.5 pips past signal + max_slippage
        await env.settle()
        alerts = [e for e in env.rec.of(AlertEvent) if e.kind == "slippage_exceeded"]
        assert [a.message.split(" (")[0] for a in alerts] == [
            "ema BUY 0.10 EURUSD STOP_LIMIT filled at 1.10045, 0.00015 beyond its price limit 1.10030",
            "ema BUY 0.10 EURUSD MARKET filled at 1.10025, 0.00015 beyond its price limit 1.10010",
        ]
    finally:
        await env.close()
