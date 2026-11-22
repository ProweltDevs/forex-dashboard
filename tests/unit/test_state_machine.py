"""Exhaustive and property tests for the order state machine (docs/SPEC.md §3)."""

from __future__ import annotations

import itertools
from datetime import timedelta
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.factories import T0, new_order
from trader.domain import Order, OrderStatus
from trader.domain.errors import InvalidOrderTransition
from trader.oms.state_machine import TRANSITIONS, apply_fill, can_transition, transition

S = OrderStatus
FILL_STATES = {S.PARTIALLY_FILLED, S.FILLED}

# The spec's table, written out independently of the implementation.
EXPECTED: set[tuple[S, S]] = {
    (S.NEW, S.PENDING_SUBMIT),
    (S.PENDING_SUBMIT, S.ACCEPTED),
    (S.PENDING_SUBMIT, S.UNKNOWN),
    (S.ACCEPTED, S.WORKING),
    (S.ACCEPTED, S.PARTIALLY_FILLED),
    (S.ACCEPTED, S.FILLED),
    (S.ACCEPTED, S.UNKNOWN),
    (S.WORKING, S.PARTIALLY_FILLED),
    (S.WORKING, S.FILLED),
    (S.WORKING, S.UNKNOWN),
    (S.PARTIALLY_FILLED, S.PARTIALLY_FILLED),
    (S.PARTIALLY_FILLED, S.FILLED),
    (S.PARTIALLY_FILLED, S.UNKNOWN),
    (S.UNKNOWN, S.ACCEPTED),
    (S.UNKNOWN, S.WORKING),
    (S.UNKNOWN, S.PARTIALLY_FILLED),
    (S.UNKNOWN, S.FILLED),
} | {(src, dst) for src in S if not src.is_terminal for dst in (S.CANCELLED, S.REJECTED, S.EXPIRED)}


def order_in(status: S) -> Order:
    """Build an order in ``status`` without going through the machine."""
    base = new_order()
    filled = {S.PARTIALLY_FILLED: Decimal("0.5"), S.FILLED: Decimal(1)}.get(status, Decimal(0))
    return Order(
        request=base.request,
        status=status,
        history=(
            *base.history,
            *(() if status is S.NEW else (base.history[0].__class__(status=status, ts=T0),)),
        ),
        filled_volume=filled,
        avg_fill_price=Decimal("1.1") if filled else None,
    )


def test_table_covers_every_status() -> None:
    assert set(TRANSITIONS) == set(S)


def test_transition_table_matches_spec() -> None:
    actual = {(src, dst) for src, dsts in TRANSITIONS.items() for dst in dsts}
    assert actual == EXPECTED


@pytest.mark.parametrize(("src", "dst"), list(itertools.product(S, S)))
def test_every_pair(src: S, dst: S) -> None:
    order = order_in(src)
    later = T0 + timedelta(seconds=1)
    allowed = (src, dst) in EXPECTED
    assert can_transition(src, dst) is allowed

    if dst in FILL_STATES:
        # Fill states are only reachable through apply_fill.
        with pytest.raises(ValueError, match="apply_fill"):
            transition(order, dst, later)
        volume = order.remaining_volume if dst is S.FILLED else min(Decimal("0.1"), order.remaining_volume)
        if volume <= 0 or (dst is S.PARTIALLY_FILLED and volume == order.remaining_volume):
            return
        if allowed:
            assert apply_fill(order, volume, Decimal("1.2"), later).status is dst
        else:
            with pytest.raises(InvalidOrderTransition):
                apply_fill(order, volume, Decimal("1.2"), later)
        return

    if allowed:
        moved = transition(order, dst, later, reason="r")
        assert moved.status is dst
        assert moved.history[-1].status is dst
        assert moved.history[:-1] == order.history
    else:
        with pytest.raises(InvalidOrderTransition) as info:
            transition(order, dst, later)
        assert (info.value.current, info.value.target) == (src, dst)


def test_terminal_states_are_absorbing() -> None:
    for status in S:
        if status.is_terminal:
            assert TRANSITIONS[status] == frozenset()


def test_rejection_reason_and_broker_id_recorded() -> None:
    order = transition(new_order(), S.PENDING_SUBMIT, T0)
    order = transition(order, S.ACCEPTED, T0, broker_order_id="B1")
    assert order.broker_order_id == "B1"
    rejected = transition(order, S.REJECTED, T0, reason="EXECUTION_REQUOTE")
    assert rejected.rejection_reason == "EXECUTION_REQUOTE"


def test_timestamps_must_not_go_backwards() -> None:
    order = transition(new_order(), S.PENDING_SUBMIT, T0 + timedelta(seconds=5))
    with pytest.raises(ValueError, match="before"):
        transition(order, S.ACCEPTED, T0)


def test_overfill_rejected() -> None:
    order = transition(transition(new_order(), S.PENDING_SUBMIT, T0), S.ACCEPTED, T0)
    with pytest.raises(ValueError, match="overfill"):
        apply_fill(order, Decimal("1.01"), Decimal("1.1"), T0)


fills = st.lists(
    st.tuples(
        st.integers(min_value=1, max_value=50),  # volume in 0.01 lots
        st.decimals(min_value=Decimal("0.5"), max_value=Decimal(2), places=5),
    ),
    min_size=1,
    max_size=20,
)


@given(fills)
def test_vwap_and_status_after_fills(parts: list[tuple[int, Decimal]]) -> None:
    volumes = [Decimal(v) / 100 for v, _ in parts]
    total = sum(volumes, Decimal(0))
    order = Order.new(new_order(total).request, T0)
    order = transition(transition(order, S.PENDING_SUBMIT, T0), S.ACCEPTED, T0)
    for vol, (_, price) in zip(volumes, parts, strict=True):
        order = apply_fill(order, vol, price, T0)

    assert order.status is S.FILLED
    assert order.filled_volume == total
    expected = sum((v * p for v, (_, p) in zip(volumes, parts, strict=True)), Decimal(0)) / total
    assert order.avg_fill_price is not None
    assert abs(order.avg_fill_price - expected) < Decimal("1e-20")
    lo, hi = min(p for _, p in parts), max(p for _, p in parts)
    assert lo <= order.avg_fill_price <= hi


@given(st.lists(st.sampled_from(list(S)), max_size=30))
def test_random_walks_never_leave_terminal(steps: list[S]) -> None:
    order = new_order()
    ts = T0
    for target in steps:
        ts += timedelta(milliseconds=1)
        try:
            if target in FILL_STATES:
                vol = order.remaining_volume if target is S.FILLED else order.remaining_volume / 2
                order = apply_fill(order, vol, Decimal("1.1"), ts)
            else:
                order = transition(order, target, ts)
        except (InvalidOrderTransition, ValueError):
            continue
    statuses = [c.status for c in order.history]
    first_terminal = next((i for i, s in enumerate(statuses) if s.is_terminal), None)
    if first_terminal is not None:
        assert first_terminal == len(statuses) - 1
    for prev, nxt in itertools.pairwise(statuses):
        assert can_transition(prev, nxt)
