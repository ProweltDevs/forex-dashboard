from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.factories import eurusd
from trader.domain import Position, Side, Tick
from trader.domain.exits import BreakEven, ScaleLevel, ScaleOut, TimeExit, TrailingStop
from trader.oms.emulation import (
    CancelOrder,
    ClosePosition,
    EmulationEngine,
    ModifyStop,
    SendStopLimit,
    decode,
    encode,
)

D = Decimal
T0 = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)
INST = {"EURUSD": eurusd()}


def tick(bid: str, ts: datetime, spread: str = "0.00010") -> Tick:
    b = D(bid)
    return Tick(symbol="EURUSD", bid=b, ask=b + D(spread), ts=ts, recv_ts=ts, recv_mono_ns=0)


def pos(side: Side = Side.BUY, volume: str = "1", sl: str | None = None) -> dict[str, Position]:
    p = Position(
        position_id="P1",
        symbol="EURUSD",
        side=side,
        volume=D(volume),
        entry_price=D("1.10000"),
        opened_ts=T0,
        strategy_id="s",
        sl=D(sl) if sl else None,
    )
    return {"P1": p}


def arm(engine: EmulationEngine, *exits: object, side: Side = Side.BUY, volume: str = "1") -> None:
    engine.arm_exits(
        position_id="P1",
        symbol="EURUSD",
        side=side,
        entry=D("1.10000"),
        volume=D(volume),
        opened=T0,
        exits=exits,
    )  # type: ignore[arg-type]


def test_trailing_activation_steps_throttle_and_never_backwards() -> None:
    eng = EmulationEngine(min_modify_interval=timedelta(seconds=1))
    arm(eng, TrailingStop(distance=D("0.00200"), activation=D("0.00100"), step=D("0.00050")))
    positions = pos()
    assert eng.on_tick(tick("1.10050", T0), positions, INST) == []  # not activated
    acts = eng.on_tick(tick("1.10100", T0 + timedelta(seconds=1)), positions, INST)
    assert acts == [ModifyStop("P1", D("1.09900"), "trailing stop")]
    assert eng.on_tick(tick("1.10120", T0 + timedelta(seconds=3)), positions, INST) == []  # < step
    assert eng.on_tick(tick("1.10200", T0 + timedelta(seconds=3, milliseconds=500)), positions, INST) == [
        ModifyStop("P1", D("1.10000"), "trailing stop")
    ]
    # Within the throttle window an improvement is held back, then sent on a later tick.
    assert eng.on_tick(tick("1.10300", T0 + timedelta(seconds=4)), positions, INST) == []
    assert eng.on_tick(tick("1.10250", T0 + timedelta(seconds=5)), positions, INST) == [
        ModifyStop("P1", D("1.10100"), "trailing stop")
    ]
    assert (
        eng.on_tick(tick("1.10000", T0 + timedelta(seconds=9)), positions, INST) == []
    )  # price fell: no move back


def test_trailing_short_and_external_sl_respected() -> None:
    eng = EmulationEngine(min_modify_interval=timedelta(0))
    arm(eng, TrailingStop(distance=D("0.00100")), side=Side.SELL)
    positions = pos(Side.SELL)
    acts = eng.on_tick(tick("1.09790", T0), positions, INST)  # ask 1.09800 -> stop 1.09900
    assert acts == [ModifyStop("P1", D("1.09900"), "trailing stop")]
    eng.note_stop("P1", D("1.09850"))  # tightened by hand: trailing must not loosen it
    assert eng.on_tick(tick("1.09770", T0 + timedelta(seconds=1)), positions, INST) == []
    assert eng.on_tick(tick("1.09700", T0 + timedelta(seconds=2)), positions, INST) == [
        ModifyStop("P1", D("1.09810"), "trailing stop")
    ]


def test_break_even_once_and_only_if_better() -> None:
    eng = EmulationEngine()
    arm(eng, BreakEven(trigger=D("0.00150"), offset=D("0.00010")))
    assert eng.on_tick(tick("1.10100", T0), pos(), INST) == []
    assert eng.on_tick(tick("1.10160", T0), pos(), INST) == [ModifyStop("P1", D("1.10010"), "break-even")]
    assert eng.on_tick(tick("1.10300", T0), pos(), INST) == []  # done
    eng2 = EmulationEngine()
    arm(eng2, BreakEven(trigger=D("0.00150")))
    assert eng2.on_tick(tick("1.10200", T0), pos(sl="1.10050"), INST) == []  # SL already better


def test_time_exit_by_tick_or_timer() -> None:
    eng = EmulationEngine()
    arm(eng, TimeExit(after=timedelta(minutes=30)))
    assert eng.on_time(T0 + timedelta(minutes=29)) == []
    assert eng.on_time(T0 + timedelta(minutes=30)) == [ClosePosition("P1", None, "time exit")]
    assert eng.on_time(T0 + timedelta(minutes=31)) == []


def test_scale_out_rounds_down_and_last_level_closes_rest() -> None:
    eng = EmulationEngine()
    ladder = ScaleOut(
        levels=(
            ScaleLevel(distance=D("0.001"), fraction=D("0.333")),
            ScaleLevel(distance=D("0.002"), fraction=D("0.333")),
            ScaleLevel(distance=D("0.003"), fraction=D("0.334")),
        )
    )
    arm(eng, ladder, volume="0.10")
    positions = pos(volume="0.10")
    assert eng.on_tick(tick("1.10100", T0), positions, INST) == [
        ClosePosition("P1", D("0.03"), "scale-out level 1")
    ]
    positions = pos(volume="0.07")
    assert eng.on_tick(tick("1.10200", T0), positions, INST) == [
        ClosePosition("P1", D("0.03"), "scale-out level 2")
    ]
    positions = pos(volume="0.04")
    assert eng.on_tick(tick("1.10350", T0), positions, INST) == [
        ClosePosition("P1", None, "scale-out level 3 (rest)")
    ]


def test_scale_out_gap_through_levels_and_min_volume() -> None:
    eng = EmulationEngine()
    arm(
        eng,
        ScaleOut(
            levels=(
                ScaleLevel(distance=D("0.001"), fraction=D("0.5")),
                ScaleLevel(distance=D("0.002"), fraction=D("0.5")),
            )
        ),
        volume="0.03",
    )
    acts = eng.on_tick(tick("1.10500", T0), pos(volume="0.03"), INST)  # gaps through both levels
    assert acts == [
        ClosePosition("P1", D("0.01"), "scale-out level 1"),
        ClosePosition("P1", None, "scale-out level 2 (rest)"),
    ]


def test_emulated_sl_tp_expiry_and_stop_limit() -> None:
    eng = EmulationEngine()
    eng.arm_sl_tp(position_id="P1", symbol="EURUSD", side=Side.BUY, sl=D("1.09900"), tp=D("1.10200"))
    assert eng.on_tick(tick("1.10000", T0), pos(), INST) == []
    assert eng.on_tick(tick("1.09890", T0), pos(), INST) == [
        ClosePosition("P1", None, "emulated stop loss 1.09900")
    ]
    eng.arm_expiry(client_order_id="C1", symbol="EURUSD", deadline=T0 + timedelta(hours=1))
    assert eng.on_time(T0 + timedelta(hours=1)) == [CancelOrder("C1", "expired (emulated)")]
    eng.arm_stop_limit(client_order_id="C2", symbol="EURUSD", side=Side.BUY, stop=D("1.10100"))
    assert eng.on_tick(tick("1.10080", T0), {}, INST) == []  # ask 1.10090
    assert eng.on_tick(tick("1.10090", T0), {}, INST) == [SendStopLimit("C2", "stop 1.10100 reached")]
    assert eng.on_tick(tick("1.10200", T0), {}, INST) == []  # triggered once
    assert eng.drop_order("C2") == 1


def test_codec_round_trip_and_reload() -> None:
    eng = EmulationEngine()
    arm(
        eng,
        TrailingStop(distance=D("0.002")),
        BreakEven(trigger=D("0.001")),
        TimeExit(after=timedelta(minutes=5)),
        ScaleOut(levels=(ScaleLevel(distance=D("0.001"), fraction=D("1")),)),
    )
    eng.arm_sl_tp(position_id="P1", symbol="EURUSD", side=Side.SELL, sl=D("1.2"), tp=None)
    eng.arm_expiry(client_order_id="C1", symbol="EURUSD", deadline=T0)
    eng.arm_stop_limit(client_order_id="C2", symbol="EURUSD", side=Side.SELL, stop=D("1.05"))
    dumped = eng.dump()
    for raw in dumped:
        assert encode(decode(raw)) == raw
    other = EmulationEngine()
    other.load(dumped)
    assert other.dump() == dumped
    new = other.arm_expiry(client_order_id="C9", symbol="EURUSD", deadline=T0)
    assert new.id not in {str(r["id"]) for r in dumped}  # ids keep counting after a reload
    assert other.drop_position("P1") == 5


def test_exit_spec_validation() -> None:
    with pytest.raises(ValueError, match="distance"):
        TrailingStop(distance=D(0))
    with pytest.raises(ValueError, match="offset"):
        BreakEven(trigger=D("0.001"), offset=D("0.002"))
    with pytest.raises(ValueError, match="increasing"):
        ScaleOut(
            levels=(
                ScaleLevel(distance=D(2), fraction=D("0.5")),
                ScaleLevel(distance=D(1), fraction=D("0.5")),
            )
        )
    with pytest.raises(ValueError, match="sum"):
        ScaleOut(
            levels=(
                ScaleLevel(distance=D(1), fraction=D("0.6")),
                ScaleLevel(distance=D(2), fraction=D("0.6")),
            )
        )
