"""Example strategies, driven bar by bar through the harness (deterministic: no clock, no broker)."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from tests.factories import eurusd
from tests.strategy_harness import Harness
from trader.domain import Side, Timeframe
from trader.domain.exits import ScaleLevel, ScaleOut, TimeExit
from trader.risk import sizing
from trader.strategy.context import ClosePosition, Submit
from trader.strategy.examples.donchian_breakout import DonchianBreakout
from trader.strategy.examples.ema_cross import EmaCross
from trader.strategy.examples.rsi_reversion import RsiReversion
from trader.strategy.indicators import ATR
from trader.strategy.registry import BUILTIN, resolve

D = Decimal
EURUSD = eurusd()


def path(start: str, step: str, n: int) -> list[str]:
    s, d = D(start), D(step)
    return [str(s + d * i) for i in range(n)]


def until_signal(h: Harness, closes: list[str]) -> tuple[list[object], Decimal, Decimal]:
    """Push bars until one produces commands; return them with that bar's ask and ATR(3)."""
    for c in closes:
        cmds = h.push(c)
        if cmds:
            atr = h.ctx.indicator(ATR, "EURUSD", Timeframe.M5, period=3).value
            assert atr is not None
            return list(cmds), h.env.ticks["EURUSD"].ask, atr
    raise AssertionError("no signal")


def risk_ok(intent_volume: Decimal, entry: Decimal, sl: Decimal, risk_pct: Decimal, equity: Decimal) -> None:
    """Sized so the stop loses at most risk_pct % of equity, and within one volume step of it."""
    per_lot = sizing.risk_per_lot(EURUSD, entry, sl, D(1))
    budget = equity * risk_pct / 100
    assert per_lot * intent_volume <= budget
    assert per_lot * (intent_volume + EURUSD.volume_step) > budget


def test_ema_cross_enters_on_the_cross_with_atr_stops_and_reverses() -> None:
    h = Harness(EmaCross, {"fast": 3, "slow": 6, "atr_period": 3})
    h.start()
    falling = h.run(path("1.10000", "-0.00100", 12))
    assert falling == []  # only crosses trade, and the first ready bar is not a cross
    [cmd], ask, atr = until_signal(h, path("1.08900", "0.00150", 6))
    assert isinstance(cmd, Submit)
    intent = cmd.intent
    assert (intent.side, intent.strategy_id, intent.signal_price) == (Side.BUY, "test", ask)
    assert intent.sl == EURUSD.round_price(ask - 2 * atr)
    assert intent.tp is not None
    risk_ok(intent.volume, ask, intent.sl, D("0.5"), D(10_000))
    pos = h.fill(cmd)
    assert h.run(path("1.09800", "0.00100", 3)) == []  # already long: no new entry
    reversal = [cmd for _, cmd in h.run(path("1.10000", "-0.00200", 8))]
    assert isinstance(reversal[0], ClosePosition)
    assert reversal[0].position_id == pos.position_id
    assert isinstance(reversal[1], Submit)
    assert reversal[1].intent.side is Side.SELL


def test_ema_cross_long_only() -> None:
    h = Harness(EmaCross, {"fast": 3, "slow": 6, "atr_period": 3, "allow_short": False})
    h.start()
    cmds = [c for _, c in h.run(path("1.10000", "0.00100", 12) + path("1.11100", "-0.00200", 8))]
    assert all(not (isinstance(c, Submit) and c.intent.side is Side.SELL) for c in cmds)


def test_donchian_breaks_out_with_a_time_exit_and_one_position_at_a_time() -> None:
    h = Harness(DonchianBreakout, {"period": 5, "atr_period": 3, "exit_after_bars": 12})
    h.start()
    ranging = ["1.10000", "1.10050", "1.09980", "1.10030", "1.10010", "1.10040", "1.10000"]
    assert h.run(ranging) == []
    [cmd] = h.push("1.10200")  # closes above the previous 5 bars' high
    assert isinstance(cmd, Submit)
    assert cmd.intent.side is Side.BUY
    assert cmd.intent.exits == (TimeExit(after=timedelta(minutes=5) * 12),)
    h.fill(cmd)
    assert h.push("1.10400") == []  # holding: no pyramiding


def test_rsi_reversion_buys_the_cross_back_above_oversold_with_a_scale_out_ladder() -> None:
    h = Harness(
        RsiReversion,
        {"rsi_period": 5, "atr_period": 3, "scale_atr": ["1", "2"], "scale_fractions": ["0.5", "0.5"]},
    )
    h.start()
    cmds = h.run(path("1.10000", "-0.00100", 10))  # RSI pinned near 0
    assert cmds == []
    [cmd], _, atr = until_signal(h, ["1.09200", "1.09400", "1.09600", "1.09800"])
    assert isinstance(cmd, Submit)
    assert cmd.intent.side is Side.BUY
    [ladder] = cmd.intent.exits
    assert isinstance(ladder, ScaleOut)
    assert ladder.levels == (
        ScaleLevel(distance=atr, fraction=D("0.5")),
        ScaleLevel(distance=2 * atr, fraction=D("0.5")),
    )
    assert h.ctx.state["rsi:EURUSD"]  # last RSI persisted as a string (JSON-safe)


def test_registry_and_param_validation() -> None:
    for name in BUILTIN:
        assert resolve(name).Params.model_validate({}) is not None
    assert resolve("trader.strategy.examples.ema_cross:EmaCross") is EmaCross
    for bad in ("nope", "trader.strategy.base:Params", "x:"):
        try:
            resolve(bad)
        except (ValueError, ModuleNotFoundError):
            continue
        raise AssertionError(bad)
    with pytest.raises(ValueError, match="fast"):
        EmaCross.Params.model_validate({"fast": 30, "slow": 10})
