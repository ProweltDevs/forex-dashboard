"""Property tests on the OMS (phase 5 acceptance): whatever order the broker's events arrive in,
duplicated or stale, the OMS never moves an order backwards, terminal states absorb, filled volume never
exceeds the order or decreases, a re-delivered fill changes nothing, and no event crashes the writer."""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from structlog.testing import capture_logs

from tests.integration.oms_helpers import buy, make_env
from trader.domain import Order, OrderStatus, OrderType
from trader.domain.enums import Side
from trader.domain.events import DomainEvent, FillEvent, OrderEvent
from trader.domain.models import Fill, StatusChange
from trader.oms.state_machine import can_transition

D = Decimal
S = OrderStatus
LOT = D("0.01")
VOLUME = D("0.10")
RANK = {S.NEW: 0, S.PENDING_SUBMIT: 1, S.ACCEPTED: 2, S.WORKING: 3, S.PARTIALLY_FILLED: 4}


@dataclass(frozen=True)
class View:
    status: OrderStatus
    filled_lots: int
    by_broker_id_only: bool


@dataclass(frozen=True)
class Exec:
    fill_id: str
    lots: int


Step = View | Exec

views = st.builds(
    View,
    status=st.sampled_from(
        [S.ACCEPTED, S.WORKING, S.PARTIALLY_FILLED, S.FILLED, S.CANCELLED, S.REJECTED, S.EXPIRED]
    ),
    filled_lots=st.integers(0, 10),
    by_broker_id_only=st.booleans(),
)
execs = st.builds(Exec, fill_id=st.sampled_from(["d1", "d2", "d3", "d4", "d5"]), lots=st.integers(1, 6))
steps = st.lists(st.one_of(views, execs), min_size=1, max_size=25)


def rank(status: OrderStatus) -> int:
    return 9 if status.is_terminal else RANK[status]


def to_event(step: Step, working: Order) -> DomainEvent:
    now = datetime.now(UTC)
    bid = working.broker_order_id
    assert bid is not None
    if isinstance(step, Exec):
        fill = Fill(
            fill_id=step.fill_id,
            client_order_id=None,
            broker_order_id=bid,
            position_id="P1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=LOT * step.lots,
            price=D("1.05"),
            ts=now,
        )
        return FillEvent(account_id="acct", fill=fill, ts=now)
    request = working.request
    if step.by_broker_id_only:  # e.g. an adapter that lost the client id: a fallback id + the broker id
        request = replace(request, client_order_id=f"stub-{bid}")
    filled = LOT * step.filled_lots
    view = Order(
        request=request,
        status=step.status,
        history=(StatusChange(status=step.status, ts=now),),
        broker_order_id=bid,
        filled_volume=filled,
        avg_fill_price=D("1.05") if filled else None,
    )
    return OrderEvent(account_id="acct", order=view, previous_status=None, ts=now)


async def scenario(seq: list[Step]) -> None:
    env = await make_env()
    try:
        # A resting limit far from the market: the paper broker will not touch it again.
        working = await env.oms.submit(buy(str(VOLUME), order_type=OrderType.LIMIT, limit_price=D("1.05")))
        await env.oms.flush()
        cid = working.client_order_id
        current = env.oms.order(cid)
        assert current is not None
        assert current.status is S.WORKING
        seen: set[str] = set()
        for step in seq:
            prev = current
            env.oms.on_broker_event(to_event(step, prev))
            await env.oms.flush()
            current = env.oms.order(cid)
            assert current is not None
            if prev.status.is_terminal:  # terminal absorbs
                assert (current.status, current.filled_volume) == (prev.status, prev.filled_volume)
            assert rank(current.status) >= rank(prev.status)  # never backwards
            assert prev.filled_volume <= current.filled_volume <= current.request.volume
            if current.status is S.FILLED:
                assert current.filled_volume == current.request.volume
            if current.status is S.PARTIALLY_FILLED:
                assert 0 < current.filled_volume < current.request.volume
            if isinstance(step, Exec):
                if step.fill_id in seen:  # a re-delivered deal changes nothing
                    assert (current.status, current.filled_volume) == (prev.status, prev.filled_volume)
                seen.add(step.fill_id)
            statuses = [h.status for h in current.history]
            assert all(can_transition(a, b) for a, b in itertools.pairwise(statuses))
    finally:
        await env.close()


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(steps)
def test_oms_order_state_is_monotonic_under_any_event_sequence(seq: list[Step]) -> None:
    with capture_logs() as logs:
        asyncio.run(scenario(seq))
    assert [e for e in logs if e.get("event") == "oms_job_failed"] == []
