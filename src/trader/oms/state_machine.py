"""Order state machine (docs/SPEC.md §3). Pure functions over immutable ``Order`` values.

    NEW -submit-> PENDING_SUBMIT -ack-> ACCEPTED -(pending types)-> WORKING
    PENDING_SUBMIT -timeout-> UNKNOWN -reconcile-> (any real state)
    ACCEPTED|WORKING -partial-> PARTIALLY_FILLED -> FILLED   (ACCEPTED|WORKING -> FILLED for full fills)
    any non-terminal -> CANCELLED | REJECTED | EXPIRED
    Terminal: FILLED, CANCELLED, REJECTED, EXPIRED

See docs/adr/0002-order-state-machine.md for the edges added beyond the spec diagram.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from trader.domain.enums import OrderStatus as S
from trader.domain.errors import InvalidOrderTransition
from trader.domain.models import Order, StatusChange
from trader.domain.money import require_positive
from trader.domain.time import ensure_utc

_END = frozenset({S.CANCELLED, S.REJECTED, S.EXPIRED})

TRANSITIONS: dict[S, frozenset[S]] = {
    S.NEW: frozenset({S.PENDING_SUBMIT}) | _END,
    S.PENDING_SUBMIT: frozenset({S.ACCEPTED, S.UNKNOWN}) | _END,
    S.ACCEPTED: frozenset({S.WORKING, S.PARTIALLY_FILLED, S.FILLED, S.UNKNOWN}) | _END,
    S.WORKING: frozenset({S.PARTIALLY_FILLED, S.FILLED, S.UNKNOWN}) | _END,
    S.PARTIALLY_FILLED: frozenset({S.PARTIALLY_FILLED, S.FILLED, S.UNKNOWN}) | _END,
    # Reconcile resolves UNKNOWN to whatever the broker says is real.
    S.UNKNOWN: frozenset({S.ACCEPTED, S.WORKING, S.PARTIALLY_FILLED, S.FILLED}) | _END,
    S.FILLED: frozenset(),
    S.CANCELLED: frozenset(),
    S.REJECTED: frozenset(),
    S.EXPIRED: frozenset(),
}

# Fill-driven states must go through apply_fill() so volumes stay consistent.
_FILL_STATES = frozenset({S.PARTIALLY_FILLED, S.FILLED})


def can_transition(current: S, target: S) -> bool:
    return target in TRANSITIONS[current]


def _check(order: Order, target: S) -> None:
    if not can_transition(order.status, target):
        raise InvalidOrderTransition(order.status, target, order.client_order_id)


def transition(
    order: Order,
    target: S,
    ts: datetime,
    *,
    reason: str | None = None,
    broker_order_id: str | None = None,
) -> Order:
    """Move ``order`` to ``target`` (non-fill transitions). Raises ``InvalidOrderTransition``."""
    if target in _FILL_STATES:
        raise ValueError(f"use apply_fill() to move an order to {target}")
    _check(order, target)
    ts = ensure_utc(ts)
    if ts < order.updated_ts:
        raise ValueError(f"transition timestamp {ts} is before the last change {order.updated_ts}")
    return replace(
        order,
        status=target,
        history=(*order.history, StatusChange(status=target, ts=ts, reason=reason)),
        broker_order_id=broker_order_id if broker_order_id is not None else order.broker_order_id,
        rejection_reason=reason if target is S.REJECTED else order.rejection_reason,
    )


def apply_fill(order: Order, volume: Decimal, price: Decimal, ts: datetime) -> Order:
    """Record an execution of ``volume`` lots at ``price``: update filled volume and the
    volume-weighted average price, and move to PARTIALLY_FILLED or FILLED."""
    require_positive("volume", volume)
    require_positive("price", price)
    new_filled = order.filled_volume + volume
    if new_filled > order.request.volume:
        raise ValueError(
            f"overfill on {order.client_order_id}: {new_filled} > order volume {order.request.volume}"
        )
    target = S.FILLED if new_filled == order.request.volume else S.PARTIALLY_FILLED
    _check(order, target)
    ts = ensure_utc(ts)
    if ts < order.updated_ts:
        raise ValueError(f"fill timestamp {ts} is before the last change {order.updated_ts}")

    prev_notional = order.filled_volume * (order.avg_fill_price or Decimal(0))
    avg = (prev_notional + volume * price) / new_filled
    return replace(
        order,
        status=target,
        history=(*order.history, StatusChange(status=target, ts=ts)),
        filled_volume=new_filled,
        avg_fill_price=avg,
    )
