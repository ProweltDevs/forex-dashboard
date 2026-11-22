"""OrderManager + AccountRuntime against the paper broker."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.integration.oms_helpers import Env, RejectAbove, buy, make_env, settings
from trader.brokers.base import OrderChanges
from trader.domain import (
    ConnectionEvent,
    ConnectionState,
    FillEvent,
    OrderEvent,
    OrderStatus,
    OrderType,
    PositionChange,
    PositionEvent,
    ReconcileEvent,
    RiskDecision,
    RiskEvent,
    Side,
    TimeInForce,
)
from trader.domain.errors import BrokerUnavailable, OrderNotFound
from trader.domain.exits import TimeExit, TrailingStop
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest

D = Decimal


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    e = await make_env()
    yield e
    await e.close()


def statuses(env: Env, cid: str) -> list[OrderStatus]:
    return [h.status for h in env.oms.order(cid).history]  # type: ignore[union-attr]


async def filled(env: Env, cid: str) -> None:
    await env.rec.wait(
        OrderEvent, lambda e: e.order.client_order_id == cid and e.order.status is OrderStatus.FILLED
    )


# ------------------------------------------------------------------ lifecycle


async def test_market_order_lifecycle_and_attribution(env: Env) -> None:
    order = await env.oms.submit(buy())
    assert order.status is OrderStatus.ACCEPTED
    cid = order.client_order_id
    await filled(env, cid)
    assert statuses(env, cid) == [
        OrderStatus.NEW,
        OrderStatus.PENDING_SUBMIT,
        OrderStatus.ACCEPTED,
        OrderStatus.FILLED,
    ]
    fill = await env.rec.wait(FillEvent, lambda e: e.fill.client_order_id == cid)
    assert fill.fill.price == D("1.10010")
    pos = await env.rec.wait(PositionEvent, lambda e: e.change is PositionChange.OPENED)
    assert pos.position.strategy_id == "ema"
    risk = env.rec.of(RiskEvent)[0]
    assert (risk.decision, risk.client_order_id) == (RiskDecision.APPROVED, cid)
    assert env.broker.sent[0].client_order_id == cid
    assert env.store.saves > 0  # type: ignore[attr-defined]


async def test_risk_rejection_never_reaches_broker() -> None:
    env = await make_env(risk=RejectAbove(D("0.05")))
    try:
        order = await env.oms.submit(buy("0.10"))
        assert order.status is OrderStatus.REJECTED
        assert order.rejection_reason is not None
        assert order.rejection_reason.startswith("risk:max_volume")
        assert env.broker.sent == []
        assert env.rec.of(RiskEvent)[0].decision is RiskDecision.REJECTED
    finally:
        await env.close()


async def test_pending_modify_cancel(env: Env) -> None:
    order = await env.oms.submit(buy(order_type=OrderType.LIMIT, limit_price=D("1.09000")))
    cid = order.client_order_id
    await env.rec.wait(
        OrderEvent, lambda e: e.order.client_order_id == cid and e.order.status is OrderStatus.WORKING
    )
    await env.oms.modify_order(cid, OrderChanges(limit_price=D("1.08900")))
    await env.rec.wait(
        OrderEvent, lambda e: e.order.client_order_id == cid and e.order.request.limit_price == D("1.08900")
    )
    await env.oms.cancel_order(cid)
    await env.rec.wait(
        OrderEvent, lambda e: e.order.client_order_id == cid and e.order.status is OrderStatus.CANCELLED
    )
    with pytest.raises(OrderNotFound):
        await env.oms.cancel_order(cid)


# ------------------------------------------------------------------ failures


async def test_retryable_error_retries_with_same_id(env: Env) -> None:
    env.broker.script = ["requote", "ok"]
    order = await env.oms.submit(buy())
    await filled(env, order.client_order_id)
    assert [r.client_order_id for r in env.broker.sent] == [order.client_order_id]
    assert len(await env.paper.positions()) == 1


async def test_non_retryable_error_rejects(env: Env) -> None:
    env.broker.script = ["invalid"]
    order = await env.oms.submit(buy())
    assert order.status is OrderStatus.REJECTED
    assert "InvalidVolume" in (order.rejection_reason or "")


async def test_ack_timeout_goes_unknown_then_resolves(env: Env) -> None:
    env.broker.script = ["hang_after"]
    order = await env.oms.submit(buy())
    assert order.status in (OrderStatus.UNKNOWN, OrderStatus.ACCEPTED, OrderStatus.FILLED)
    await filled(env, order.client_order_id)
    assert len(env.broker.sent) == 1  # resolved by events/lookup, never resent
    assert len(await env.paper.positions()) == 1


async def test_unknown_not_at_broker_is_resubmitted_once(env: Env) -> None:
    env.broker.script = ["unknown_before"]
    order = await env.oms.submit(buy())
    assert order.status is OrderStatus.UNKNOWN
    await filled(env, order.client_order_id)
    kinds = [e.kind for e in env.rec.of(ReconcileEvent)]
    assert "order_resubmitted" in kinds
    assert len(env.broker.sent) == 1
    assert len(await env.paper.positions()) == 1


async def test_non_idempotent_broker_looks_twice_before_resending() -> None:
    env = await make_env(caps={"idempotent_submit": False})
    try:
        env.broker.script = ["unknown_before"]
        order = await env.oms.submit(buy())
        await filled(env, order.client_order_id)
        assert env.broker.lookups[:2] == [order.client_order_id] * 2  # two "not found", one grace apart
        assert len(env.broker.sent) == 1
        assert len(await env.paper.positions()) == 1
    finally:
        await env.close()


async def test_unknown_and_stale_is_abandoned_not_resent() -> None:
    env = await make_env(oms_settings=settings(resubmit_window=timedelta(0)))
    try:
        env.broker.script = ["unknown_before"]
        order = await env.oms.submit(buy(signal_ts=datetime.now(UTC) - timedelta(seconds=1)))
        abandoned = await env.rec.wait(ReconcileEvent, lambda e: e.kind == "order_abandoned")
        assert abandoned.entity_id == order.client_order_id
        assert env.oms.order(order.client_order_id).status is OrderStatus.REJECTED  # type: ignore[union-attr]
        assert env.broker.sent == []
    finally:
        await env.close()


async def test_submit_refused_until_reconciled(env: Env) -> None:
    env.oms.ready.clear()
    with pytest.raises(BrokerUnavailable, match="reconciled"):
        await env.oms.submit(buy())


# ------------------------------------------------------------------ reconcile


async def test_reconnect_reconciles_changes_made_while_down(env: Env) -> None:
    order = await env.oms.submit(buy(sl=D("1.09500")))
    await filled(env, order.client_order_id)
    pid = (
        await env.rec.wait(FillEvent, lambda e: e.fill.client_order_id == order.client_order_id)
    ).fill.position_id
    # An order placed outside the OMS, then the link drops and the SL is hit broker-side.
    external = OrderRequest(
        client_order_id=new_client_order_id(),
        symbol="EURUSD",
        side=Side.SELL,
        order_type=OrderType.MARKET,
        volume=D("0.05"),
        strategy_id="manual",
        strategy_version="",
    )
    await env.paper.place_order(external)
    await env.settle()
    await env.paper.simulate_disconnect()
    await env.rec.wait(ConnectionEvent, lambda e: e.state is ConnectionState.DOWN)
    assert not env.oms.ready.is_set()
    await env.quote("1.09400")  # SL hit while we are offline
    await env.paper.simulate_reconnect()
    await env.rec.wait(ReconcileEvent, lambda e: e.kind == "position_missing" and e.entity_id == pid)
    await asyncio.wait_for(env.oms.ready.wait(), 2)
    remaining = env.oms.positions()
    assert [(p.side, p.strategy_id) for p in remaining] == [
        (Side.SELL, "manual")
    ]  # orphan kept, counts for risk


async def test_close_all_and_flatten(env: Env) -> None:
    a = await env.oms.submit(buy())
    b = await env.oms.submit(buy(strategy_id="other"))
    pending = await env.oms.submit(buy(order_type=OrderType.LIMIT, limit_price=D("1.09000")))
    await filled(env, a.client_order_id)
    await filled(env, b.client_order_id)
    await env.rec.wait(
        OrderEvent,
        lambda e: (
            e.order.client_order_id == pending.client_order_id and e.order.status is OrderStatus.WORKING
        ),
    )
    assert await env.oms.close_all(strategy_id="other") == 1
    await env.rec.wait(
        PositionEvent, lambda e: e.change is PositionChange.CLOSED and e.position.strategy_id == "other"
    )
    assert await env.oms.flatten() == 2
    await env.rec.wait(
        OrderEvent,
        lambda e: (
            e.order.client_order_id == pending.client_order_id and e.order.status is OrderStatus.CANCELLED
        ),
    )
    await env.settle(0.2)
    assert env.oms.positions() == []
    assert await env.paper.positions() == []


async def test_oco_group_cancels_sibling(env: Env) -> None:
    up = await env.oms.submit(buy(order_type=OrderType.STOP, stop_price=D("1.10100"), oco_group="breakout"))
    down = await env.oms.submit(
        buy(side=Side.SELL, order_type=OrderType.STOP, stop_price=D("1.09900"), oco_group="breakout")
    )
    for o in (up, down):
        await env.rec.wait(
            OrderEvent,
            lambda e, o=o: (
                e.order.client_order_id == o.client_order_id and e.order.status is OrderStatus.WORKING
            ),
        )
    await env.quote("1.10100")
    await filled(env, up.client_order_id)
    await env.rec.wait(
        OrderEvent,
        lambda e: e.order.client_order_id == down.client_order_id and e.order.status is OrderStatus.CANCELLED,
    )


# ------------------------------------------------------------------ emulation through the OMS


async def test_trailing_stop_moves_native_sl(env: Env) -> None:
    order = await env.oms.submit(buy(sl=D("1.09500"), exits=(TrailingStop(distance=D("0.00200")),)))
    await filled(env, order.client_order_id)
    await env.settle()
    await env.quote("1.10500")
    await env.rec.wait(PositionEvent, lambda e: e.position.sl == D("1.10300"))
    await env.quote("1.10400")  # pulls back: the stop stays
    await env.settle()
    assert env.oms.positions()[0].sl == D("1.10300")


async def test_emulated_stop_limit_sl_tp_and_expiry_when_broker_lacks_them() -> None:
    env = await make_env(
        caps={
            "order_types": frozenset({OrderType.MARKET, OrderType.LIMIT, OrderType.STOP}),
            "native_sl_tp": False,
            "server_side_expiry": False,
        }
    )
    try:
        sl = await env.oms.submit(
            buy(
                order_type=OrderType.STOP_LIMIT,
                stop_price=D("1.10100"),
                limit_price=D("1.10100"),
                sl=D("1.09950"),
                tp=D("1.10400"),
            )
        )
        assert sl.status is OrderStatus.WORKING
        assert env.broker.sent == []  # held locally until the stop triggers
        await env.quote(
            "1.10095"
        )  # ask 1.10105 >= stop; the limit (1.10100) is below the ask -> rests as LIMIT
        await env.settle()
        assert env.broker.sent[0].order_type is OrderType.LIMIT
        assert env.broker.sent[0].sl is None  # SL/TP are emulated, not sent
        await env.quote("1.10085")  # ask 1.10095 <= limit -> fills
        await filled(env, sl.client_order_id)
        await env.settle()
        await env.quote("1.09940")  # below the emulated SL -> the OMS closes
        await env.rec.wait(PositionEvent, lambda e: e.change is PositionChange.CLOSED)

        marketable = await env.oms.submit(
            buy(order_type=OrderType.STOP_LIMIT, stop_price=D("1.10000"), limit_price=D("1.10030"))
        )
        await env.quote("1.10005")  # ask 1.10015: through the stop, still under the limit -> MARKET capped
        await filled(env, marketable.client_order_id)
        leg = env.broker.sent[-1]
        assert (leg.order_type, leg.max_slippage) == (OrderType.MARKET, D("0.00015"))

        day = await env.oms.submit(
            buy(
                order_type=OrderType.LIMIT,
                limit_price=D("1.09000"),
                time_in_force=TimeInForce.GTD,
                expiry=datetime.now(UTC) + timedelta(milliseconds=200),
            )
        )
        assert env.broker.sent[-1].time_in_force is TimeInForce.GTC
        await env.rec.wait(
            OrderEvent,
            lambda e: (
                e.order.client_order_id == day.client_order_id and e.order.status is OrderStatus.CANCELLED
            ),
        )
    finally:
        await env.close()


async def test_time_exit_closes_position(env: Env) -> None:
    order = await env.oms.submit(buy(exits=(TimeExit(after=timedelta(milliseconds=200)),)))
    await filled(env, order.client_order_id)
    await env.rec.wait(PositionEvent, lambda e: e.change is PositionChange.CLOSED, timeout=5)
