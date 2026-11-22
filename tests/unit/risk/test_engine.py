"""RiskEngine: flow tracking, kill switch, circuit breakers, enforcement and persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.factories import eurusd, intent
from tests.unit.risk.test_rules import account, position, tick, view, working
from trader.config.risk import BreakerConfig, RiskConfig, RiskLimits
from trader.domain import (
    AccountEvent,
    ConnectionEvent,
    ConnectionState,
    KillLevel,
    OrderEvent,
    OrderStatus,
    ReconcileEvent,
    RiskStateEvent,
)
from trader.domain.models import Order, StatusChange, Tick
from trader.domain.time import FixedClock
from trader.risk.engine import ENFORCE_EVERY, RiskEngine
from trader.risk.gate import AccountView

D = Decimal
NOW = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)


def engine(**cfg: object) -> tuple[RiskEngine, FixedClock]:
    clock = FixedClock(NOW)
    return RiskEngine(RiskConfig.model_validate(cfg), clock, account_id="a"), clock


def ok_intent(**kw: object) -> object:
    fields: dict[str, object] = {"strategy_id": "ema", "sl": D("1.09500"), "signal_ts": NOW}
    fields.update(kw)
    return intent(**fields)


def fresh(clock: FixedClock, **kw: object) -> AccountView:
    """A view whose ticks are 1 s old at the clock's current time."""
    t = tick("EURUSD", "1.10000")
    t = replace(t, ts=clock.now() - timedelta(seconds=1), recv_ts=clock.now() - timedelta(seconds=1))
    fields: dict[str, object] = {"last_ticks": {"EURUSD": t}}
    fields.update(kw)
    return view(**fields)


def acct_event(equity: str) -> AccountEvent:
    return AccountEvent(account_id="a", state=account(equity=equity), ts=NOW)


def test_approves_then_counts_flow_and_duplicates() -> None:
    eng, clock = engine(limits={"max_orders_per_minute_strategy": 2, "duplicate_window_s": 10})
    v = fresh(clock)
    assert eng.pre_trade(ok_intent(), v).approved  # type: ignore[arg-type]
    dup = eng.pre_trade(ok_intent(), v)  # type: ignore[arg-type]
    assert (dup.approved, dup.rule) == (False, "duplicate_signal")
    clock.advance(timedelta(seconds=11))
    assert eng.pre_trade(ok_intent(), fresh(clock)).approved  # type: ignore[arg-type]
    clock.advance(timedelta(seconds=11))
    v = fresh(clock)
    capped = eng.pre_trade(ok_intent(), v)  # type: ignore[arg-type]
    assert (capped.approved, capped.rule) == (False, "orders_per_minute")
    clock.advance(timedelta(seconds=61))
    assert eng.pre_trade(ok_intent(), fresh(clock)).approved  # type: ignore[arg-type]  # window slid


def test_manual_kill_switch_blocks_entries_and_enforces_levels() -> None:
    eng, clock = engine()
    v = fresh(clock, positions=(position(),), open_orders=(working(),))
    out = eng.kill(KillLevel.PAUSE_NEW, "operator", v)
    assert eng.level is KillLevel.PAUSE_NEW
    assert [type(e) for e in out.events] == [RiskStateEvent]
    assert (out.cancel_pending, out.flatten) == (False, False)
    verdict = eng.pre_trade(ok_intent(), v)  # type: ignore[arg-type]
    assert (verdict.approved, verdict.rule) == (False, "kill_switch")
    assert "operator" in verdict.reason

    out = eng.kill(KillLevel.FLATTEN_ALL, "panic", v)
    assert (out.cancel_pending, out.flatten) == (True, True)  # immediately on escalation
    assert eng.on_time(v).flatten is False  # throttled...
    clock.advance(ENFORCE_EVERY)
    again = eng.on_time(fresh(clock, positions=(position(),), open_orders=(working(),)))
    assert (again.cancel_pending, again.flatten) == (True, True)  # ...then re-asserted while anything is left
    clock.advance(ENFORCE_EVERY)
    assert eng.on_time(fresh(clock)).flatten is False  # nothing left

    out = eng.reset("all clear", v)
    assert eng.level is KillLevel.NONE
    assert isinstance(out.events[0], RiskStateEvent)
    assert out.events[0].previous is KillLevel.FLATTEN_ALL
    with pytest.raises(ValueError, match="reset"):
        eng.kill(KillLevel.NONE, "x", v)


def test_daily_loss_flattens_and_clears_next_trading_day() -> None:
    eng, clock = engine(limits={"daily_loss_pct": 3})
    v = fresh(clock, positions=(position(),))
    eng.observe(acct_event("10000"), v)
    assert eng.day_start_equity == D(10_000)
    out = eng.observe(acct_event("9690"), replace(v, account=account(equity="9690")))
    assert eng.level is KillLevel.FLATTEN_ALL
    assert out.flatten
    assert "daily_loss" in eng.triggers
    # Equity recovering intraday does not clear it.
    clock.advance(timedelta(hours=2))
    eng.on_time(fresh(clock, account=account(equity="9900")))
    assert eng.level is KillLevel.FLATTEN_ALL
    # Next trading day: new baseline, breaker cleared.
    clock.set(datetime(2026, 1, 7, 0, 0, 1, tzinfo=UTC))
    out = eng.on_time(fresh(clock, account=account(equity="9900")))
    assert eng.level is KillLevel.NONE
    assert eng.day_start_equity == D(9900)
    assert any(isinstance(e, RiskStateEvent) and e.level is KillLevel.NONE for e in out.events)


def test_trading_day_offset() -> None:
    eng, _ = engine(day_offset_minutes=-120)  # trading days start 22:00 UTC
    assert eng.trading_day(datetime(2026, 1, 6, 21, 59, tzinfo=UTC)).day == 6
    assert eng.trading_day(datetime(2026, 1, 6, 22, 0, tzinfo=UTC)).day == 7


def test_drawdown_is_latched_and_reset_rebases_the_peak() -> None:
    eng, clock = engine(
        limits={"max_drawdown_pct": 10, "daily_loss_pct": None}, breakers={"drawdown_level": "PAUSE_NEW"}
    )
    v = fresh(clock)
    eng.observe(acct_event("12000"), v)
    eng.observe(acct_event("10700"), replace(v, account=account(equity="10700")))  # 10.8 % below the peak
    assert eng.level is KillLevel.PAUSE_NEW
    eng.on_time(fresh(clock, account=account(equity="11900")))
    assert eng.level is KillLevel.PAUSE_NEW  # latched
    eng.reset("reviewed", fresh(clock, account=account(equity="10700")))
    assert (eng.level, eng.peak_equity) == (KillLevel.NONE, D(10_700))
    eng.on_time(fresh(clock, account=account(equity="10700")))
    assert eng.level is KillLevel.NONE  # no immediate re-trigger


def _rejected() -> OrderEvent:
    req = intent().to_request("C1-test")
    hist = (
        StatusChange(status=OrderStatus.NEW, ts=NOW),
        StatusChange(status=OrderStatus.PENDING_SUBMIT, ts=NOW),
        StatusChange(status=OrderStatus.REJECTED, ts=NOW, reason="no money"),
    )
    order = Order(request=req, status=OrderStatus.REJECTED, history=hist, rejection_reason="no money")
    return OrderEvent(account_id="a", order=order, previous_status=None, ts=NOW)


def test_consecutive_broker_rejects_pause_but_risk_rejects_do_not_count() -> None:
    eng, clock = engine(breakers={"max_consecutive_rejects": 3})
    v = fresh(clock)
    risk_reject = replace(_rejected().order, history=(StatusChange(status=OrderStatus.REJECTED, ts=NOW),))
    for _ in range(5):
        eng.observe(OrderEvent(account_id="a", order=risk_reject, previous_status=None, ts=NOW), v)
    assert eng.level is KillLevel.NONE
    eng.observe(_rejected(), v)
    eng.observe(_rejected(), v)
    accepted = replace(
        _rejected().order,
        status=OrderStatus.ACCEPTED,
        history=(StatusChange(status=OrderStatus.ACCEPTED, ts=NOW),),
        rejection_reason=None,
    )
    eng.observe(OrderEvent(account_id="a", order=accepted, previous_status=None, ts=NOW), v)  # resets the run
    for _ in range(3):
        eng.observe(_rejected(), v)
    assert eng.level is KillLevel.PAUSE_NEW
    assert "3 broker rejects in a row" in eng.triggers["rejects"].reason


def test_reconcile_mismatch_pauses_only_for_configured_kinds() -> None:
    eng, clock = engine()
    v = fresh(clock)

    def rec(kind: str) -> ReconcileEvent:
        return ReconcileEvent(
            account_id="a", kind=kind, entity_id="P1", before="a", after="b", reason="x", ts=NOW
        )

    eng.observe(rec("position_adopted"), v)
    eng.observe(rec("order_resolved"), v)
    assert eng.level is KillLevel.NONE
    eng.observe(rec("position_corrected"), v)
    assert eng.level is KillLevel.PAUSE_NEW


def test_disconnect_breaker_clears_on_reconnect() -> None:
    eng, clock = engine(breakers={"disconnected_s": 30})
    down = ConnectionEvent(account_id="a", broker="paper", state=ConnectionState.DOWN, ts=NOW)
    eng.observe(down, fresh(clock, broker_connected=False))
    clock.advance(timedelta(seconds=20))
    eng.on_time(fresh(clock, broker_connected=False))
    assert eng.level is KillLevel.NONE
    clock.advance(timedelta(seconds=11))
    eng.on_time(fresh(clock, broker_connected=False))
    assert eng.level is KillLevel.PAUSE_NEW
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.NONE


def _tick_at(clock: FixedClock, *, broker_lag_ms: int = 50) -> Tick:
    now = clock.now()
    return Tick(
        symbol="EURUSD",
        bid=D("1.1"),
        ask=D("1.1001"),
        ts=now,
        recv_ts=now,
        recv_mono_ns=0,
        broker_ts=now - timedelta(milliseconds=broker_lag_ms),
    )


def test_stale_feed_breaker_respects_open_markets() -> None:
    eng, clock = engine(breakers={"feed_stale_s": 60})
    eng.on_tick(_tick_at(clock))
    clock.advance(timedelta(seconds=61))
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.PAUSE_NEW
    assert "EURUSD" in eng.triggers["feed_stale"].reason
    eng.on_tick(_tick_at(clock))
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.NONE
    # No ticks over the weekend is normal: no trigger.
    clock.set(datetime(2026, 1, 10, 12, 0, tzinfo=UTC))
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.NONE


def test_clock_drift_breaker() -> None:
    eng, clock = engine(breakers={"max_clock_drift_ms": 2000})
    for _ in range(10):
        eng.on_tick(_tick_at(clock, broker_lag_ms=2500))
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.PAUSE_NEW
    for _ in range(64):
        eng.on_tick(_tick_at(clock, broker_lag_ms=40))
    eng.on_time(fresh(clock))
    assert eng.level is KillLevel.NONE


def test_state_survives_a_restart() -> None:
    eng, clock = engine()
    v = fresh(clock)
    eng.observe(acct_event("10000"), v)
    eng.kill(KillLevel.CANCEL_PENDING, "operator", v)
    data = eng.dump()
    restored, _ = engine()
    restored.load(data)
    assert restored.level is KillLevel.CANCEL_PENDING
    assert restored.triggers["manual"].reason == "operator"
    assert (restored.day_start_equity, restored.peak_equity) == (D(10_000), D(10_000))
    assert restored.dump() == data
    assert not restored.pre_trade(ok_intent(), v).approved  # type: ignore[arg-type]


def test_breakers_can_be_switched_off() -> None:
    cfg = RiskConfig(
        limits=RiskLimits(daily_loss_pct=None, max_drawdown_pct=None),
        breakers=BreakerConfig(
            max_consecutive_rejects=None,
            feed_stale_s=None,
            disconnected_s=None,
            reconcile_kinds=(),
            max_clock_drift_ms=None,
        ),
    )
    clock = FixedClock(NOW)
    eng = RiskEngine(cfg, clock, account_id="a")
    v = fresh(clock, broker_connected=False)
    eng.observe(acct_event("10000"), v)
    eng.observe(acct_event("5000"), replace(v, account=account(equity="5000")))
    for _ in range(10):
        eng.observe(_rejected(), v)
    clock.advance(timedelta(hours=1))
    eng.on_time(fresh(clock, broker_connected=False, instruments={"EURUSD": eurusd()}))
    assert eng.level is KillLevel.NONE
