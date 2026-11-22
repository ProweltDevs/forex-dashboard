"""RiskEngine inside the OMS on the paper broker: rejects, kill-switch levels, automatic flatten on the daily
loss limit, and kill-switch state across a restart."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from tests.integration.oms_helpers import buy, make_env
from trader.brokers.paper import PaperSettings
from trader.config.risk import RiskConfig
from trader.domain import KillLevel, OrderStatus, OrderType, RiskDecision, RiskEvent, RiskStateEvent
from trader.domain.time import SystemClock
from trader.oms import JsonFileStore
from trader.risk.engine import RiskEngine

D = Decimal
# Session rules off: these tests run on any day (the weekend window would otherwise block them).
NO_SESSIONS = {"sessions": {"weekend": None, "rollover": None}}


def risk(**limits: object) -> RiskEngine:
    cfg = RiskConfig.model_validate({"limits": {**NO_SESSIONS, "duplicate_window_s": None, **limits}})
    return RiskEngine(cfg, SystemClock(), account_id="acct")


async def test_engine_rejects_and_approves_through_the_oms() -> None:
    env = await make_env(risk=risk())
    try:
        no_sl = await env.oms.submit(buy("0.10"))
        assert no_sl.status is OrderStatus.REJECTED
        assert no_sl.rejection_reason is not None
        assert no_sl.rejection_reason.startswith("risk:loss_per_trade")
        ok = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        assert ok.status is not OrderStatus.REJECTED
        decisions = [(e.decision, e.rule) for e in env.rec.of(RiskEvent)]
        assert decisions == [(RiskDecision.REJECTED, "loss_per_trade"), (RiskDecision.APPROVED, "all_rules")]
        assert env.broker.sent[0].client_order_id == ok.client_order_id  # only the approved one was sent
    finally:
        await env.close()


async def test_kill_switch_levels_pause_cancel_and_flatten() -> None:
    env = await make_env(risk=risk())
    try:
        entry = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        await env.rec.wait(RiskEvent)
        resting = await env.oms.submit(
            buy("0.10", sl=D("1.09000"), order_type=OrderType.LIMIT, limit_price=D("1.09500"))
        )
        await env.quote("1.10000")
        assert len(await env.paper.positions()) == 1
        assert env.oms.order(resting.client_order_id).status is OrderStatus.WORKING  # type: ignore[union-attr]

        assert await env.oms.kill_switch(KillLevel.PAUSE_NEW, "operator") is KillLevel.PAUSE_NEW
        blocked = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        assert blocked.rejection_reason is not None
        assert blocked.rejection_reason.startswith("risk:kill_switch")
        assert len(await env.paper.open_orders()) == 1  # PAUSE_NEW leaves pending orders alone

        await env.oms.kill_switch(KillLevel.CANCEL_PENDING, "operator")
        await env.rec.wait(RiskStateEvent, lambda e: e.level is KillLevel.CANCEL_PENDING)
        await env.settle(0.2)
        assert await env.paper.open_orders() == []
        assert len(await env.paper.positions()) == 1  # positions stay

        await env.oms.kill_switch(KillLevel.FLATTEN_ALL, "operator")
        await env.quote("1.10000")
        assert await env.paper.positions() == []
        assert env.oms.order(entry.client_order_id).status is OrderStatus.FILLED  # type: ignore[union-attr]

        assert await env.oms.reset_kill_switch("checked") is KillLevel.NONE
        again = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        assert again.status is not OrderStatus.REJECTED
        levels = [e.level for e in env.rec.of(RiskStateEvent)]
        assert levels == [
            KillLevel.PAUSE_NEW,
            KillLevel.CANCEL_PENDING,
            KillLevel.FLATTEN_ALL,
            KillLevel.NONE,
        ]
    finally:
        await env.close()


async def test_daily_loss_limit_flattens_automatically() -> None:
    env = await make_env(
        risk=risk(daily_loss=D(50), daily_loss_pct=None),
        paper_settings=PaperSettings(account_id="acct", account_event_interval=timedelta(0)),
    )
    try:
        # 0.40 lots passes every rule: 4.4x leverage, 84 USD loss at the SL (cap 1 % = 100).
        entry = await env.oms.submit(buy("0.40", sl=D("1.09800"), tp=D("1.12000")))
        assert entry.status is not OrderStatus.REJECTED, entry.rejection_reason
        await env.quote("1.10000")
        assert len(await env.paper.positions()) == 1
        await env.quote("1.09875")  # (1.09875 - 1.10010) x 40 000 = -54 USD: past the 50 USD daily limit
        state = await env.rec.wait(RiskStateEvent, lambda e: e.level is KillLevel.FLATTEN_ALL)
        assert "daily_loss" in state.triggers
        await env.quote("1.09875")
        assert await env.paper.positions() == []
        blocked = await env.oms.submit(buy("0.10", sl=D("1.09500")))
        assert blocked.rejection_reason is not None
        assert "daily_loss" in blocked.rejection_reason
    finally:
        await env.close()


async def test_kill_switch_survives_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "oms.json"
    env = await make_env(store=JsonFileStore(path), risk=risk())
    await env.oms.kill_switch(KillLevel.PAUSE_NEW, "maintenance")
    await env.close()

    restarted = await make_env(store=JsonFileStore(path), risk=risk())
    try:
        assert restarted.oms.risk_level is KillLevel.PAUSE_NEW
        order = await restarted.oms.submit(buy("0.10", sl=D("1.09500")))
        assert order.rejection_reason is not None
        assert "maintenance" in order.rejection_reason
    finally:
        await restarted.close()
