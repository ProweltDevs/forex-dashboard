from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.factories import T0, intent
from trader.domain import Order, OrderStatus, OrderType, Position, Side, TimeInForce
from trader.domain.exits import BreakEven, ScaleLevel, ScaleOut, TimeExit, TrailingStop
from trader.oms import codec
from trader.oms.state_machine import apply_fill, transition
from trader.oms.store import JsonFileStore, MemoryStore, OmsState

D = Decimal
prices = st.decimals(min_value=D("0.00001"), max_value=D(100000), places=5)


@given(prices, prices, st.sampled_from(list(Side)))
def test_request_and_order_round_trip(price: Decimal, sl: Decimal, side: Side) -> None:
    req = intent(
        side=side,
        order_type=OrderType.LIMIT,
        limit_price=price,
        sl=sl,
        time_in_force=TimeInForce.GTD,
        expiry=T0 + timedelta(hours=1),
        tags=("a", "b"),
        oco_group="g1",
        exits=(
            TrailingStop(distance=D("0.002"), step=D("0.0001")),
            BreakEven(trigger=D("0.001"), offset=D("0.0002")),
            TimeExit(after=timedelta(minutes=7)),
            ScaleOut(levels=(ScaleLevel(distance=D("0.001"), fraction=D("0.5")),)),
        ),
    ).to_request("01J00000000000000000000000")
    assert codec.request_from_json(codec.request_to_json(req)) == req
    order = Order.new(req, T0)
    for status in (OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED, OrderStatus.WORKING):
        order = transition(order, status, T0, reason="x")
    order = apply_fill(order, D("0.05"), price, T0 + timedelta(seconds=1)).with_changes(broker_order_id="B1")
    data = json.loads(json.dumps(codec.order_to_json(order)))  # through real JSON
    assert codec.order_from_json(data) == order


def test_position_round_trip() -> None:
    p = Position(
        position_id="P1",
        symbol="EURUSD",
        side=Side.SELL,
        volume=D("0.3"),
        entry_price=D("1.1"),
        opened_ts=datetime(2026, 1, 1, tzinfo=UTC),
        strategy_id="s",
        sl=D("1.2"),
        swap=D("-1.5"),
    )
    assert codec.position_from_json(codec.position_to_json(p)) == p


def test_stores(tmp_path: Path) -> None:
    state = OmsState(
        orders=[{"a": 1}], position_strategy={"P1": "s"}, fill_ids=["f1"], emulations=[{"kind": "x"}]
    )
    mem = MemoryStore()
    assert mem.load() is None
    mem.save(state)
    assert mem.load() == state
    path = tmp_path / "oms" / "acct.json"
    store = JsonFileStore(path)
    assert store.load() is None
    store.save(state)
    assert store.load() == state
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    path.write_text('{"schema": "oms/999"}')
    with pytest.raises(ValueError, match="schema"):
        store.load()
