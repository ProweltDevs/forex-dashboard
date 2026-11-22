"""StrategyRunner with fake OMS/market ports: lifecycle, routing, isolation and rollback, kill-switch pause,
timers, ownership checks and state persistence."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pytest

from tests.factories import eurusd
from trader.brokers.base import BrokerAck, OrderChanges
from trader.domain import (
    KillLevel,
    OrderEvent,
    OrderStatus,
    OrderType,
    Position,
    PositionChange,
    PositionEvent,
    RiskStateEvent,
    Side,
    StrategyEvent,
    Tick,
    TickEvent,
    Timeframe,
)
from trader.domain.events import BarEvent, DomainEvent
from trader.domain.models import AccountState, Bar, Order, OrderIntent
from trader.domain.time import FixedClock
from trader.marketdata.bus import Bus, TickBus
from trader.risk.gate import AccountView
from trader.strategy.base import Params, Strategy, StrategyStatus
from trader.strategy.context import StrategyContext
from trader.strategy.indicators import SMA
from trader.strategy.runner import StrategyRunner, StrategySpec, StrategyStateStore

D = Decimal
T0 = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)


def bar(i: int, close: str, tf: Timeframe = Timeframe.M1, symbol: str = "EURUSD") -> Bar:
    c = D(close)
    return Bar(
        symbol=symbol,
        timeframe=tf,
        start=T0 + timedelta(minutes=i),
        open=c,
        high=c,
        low=c,
        close=c,
        volume=D(1),
        complete=True,
    )


class FakeMarket:
    def __init__(self) -> None:
        self.ticks = TickBus()
        self.bars: Bus[BarEvent] = Bus()
        self.hist: dict[tuple[str, Timeframe], list[Bar]] = {}

    def history(self, broker: str, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        bars = self.hist.get((symbol, tf), [])
        return bars[-n:] if n else list(bars)

    def last_tick(self, symbol: str, broker: str | None = None) -> Tick | None:
        return self.ticks.last(symbol, broker)


class FakeOms:
    def __init__(self) -> None:
        self.events: Bus[DomainEvent] = Bus()
        self.level = KillLevel.NONE
        self.calls: list[tuple[str, object]] = []
        self.positions: tuple[Position, ...] = ()
        self.orders: tuple[Order, ...] = ()

    @property
    def risk_level(self) -> KillLevel:
        return self.level

    def view(self) -> AccountView:
        acct = AccountState(
            account_id="acct",
            currency="USD",
            balance=D(10_000),
            equity=D(10_000),
            margin=D(0),
            free_margin=D(10_000),
            leverage=100,
            ts=T0,
        )
        return AccountView(
            account_id="acct",
            account=acct,
            positions=self.positions,
            open_orders=self.orders,
            instruments={"EURUSD": eurusd()},
            last_ticks={},
            broker_connected=True,
        )

    async def submit(self, intent: OrderIntent) -> Order:
        self.calls.append(("submit", intent))
        return Order.new(intent.to_request("01J00000000000000000000000"), T0)

    async def modify_order(self, client_order_id: str, changes: OrderChanges) -> BrokerAck | None:
        self.calls.append(("modify", client_order_id))
        return None

    async def cancel_order(self, client_order_id: str, reason: str = "") -> BrokerAck | None:
        self.calls.append(("cancel", client_order_id))
        return None

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        self.calls.append(("close", position_id))
        return BrokerAck()

    async def modify_position(
        self, position_id: str, sl: Decimal | None, tp: Decimal | None
    ) -> BrokerAck | None:
        self.calls.append(("modify_position", position_id))
        return None


class P(Params):
    period: int = 3
    explode_on: str | None = None  # close price that makes on_bar raise (after queueing an order)


class Probe(Strategy[P]):
    version: ClassVar[str] = "7"
    Params: ClassVar[type[Params]] = P

    def __init__(self, strategy_id: str, params: P) -> None:
        super().__init__(strategy_id, params)
        self.seen: list[tuple[str, object]] = []
        self.sma_at_bar: list[Decimal | None] = []

    def on_start(self, ctx: StrategyContext) -> None:
        self.seen.append(("start", dict(ctx.state)))
        self.sma = ctx.indicator(SMA, "EURUSD", Timeframe.M1, period=self.params.period)

    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
        self.seen.append(("bar", bar.close))
        self.sma_at_bar.append(self.sma.value)
        ctx.state["bars"] = int(str(ctx.state.get("bars", 0))) + 1
        if self.params.explode_on is not None and bar.close == D(self.params.explode_on):
            ctx.buy("EURUSD", D("0.10"), sl=D("1.0"))
            raise RuntimeError("boom")
        if bar.close >= D(2):
            ctx.buy("EURUSD", D("0.10"), sl=D("1.0"))

    def on_order(self, ctx: StrategyContext, ev: OrderEvent) -> None:
        self.seen.append(("order", ev.order.status))

    def on_position(self, ctx: StrategyContext, ev: PositionEvent) -> None:
        self.seen.append(("position", ev.position.position_id))

    def on_timer(self, ctx: StrategyContext, name: str) -> None:
        self.seen.append(("timer", name))

    def on_stop(self, ctx: StrategyContext) -> None:
        self.seen.append(("stop", None))


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


def spec(**kw: Any) -> StrategySpec:
    fields: dict[str, Any] = {
        "id": "probe",
        "cls": Probe,
        "params": {},
        "account": "acct",
        "symbols": ("EURUSD",),
        "timeframes": (Timeframe.M1,),
        "max_errors": 2,
    }
    fields.update(kw)
    return StrategySpec(**fields)


@pytest.fixture
async def env(tmp_path: Path) -> Any:
    market, oms, clock = FakeMarket(), FakeOms(), FixedClock(T0)
    runner = StrategyRunner(
        market, {"acct": oms}, clock, StrategyStateStore(tmp_path), timer_interval=timedelta(milliseconds=5)
    )
    events: list[StrategyEvent] = []
    sub = runner.events.subscribe("t")

    async def collect() -> None:
        async for e in sub:
            events.append(e)

    task = asyncio.create_task(collect())
    await runner.run()
    yield market, oms, clock, runner, events, tmp_path
    await runner.stop_all()
    await runner.close()
    task.cancel()


def probe(runner: StrategyRunner) -> Probe:
    s = runner.handles["probe"].strategy
    assert isinstance(s, Probe)
    return s


async def test_warm_indicators_route_bars_and_submit_through_the_oms(env: Any) -> None:
    market, oms, _, runner, _, _ = env
    market.hist[("EURUSD", Timeframe.M1)] = [bar(0, "1"), bar(1, "1.3")]
    runner.add(spec())
    assert await runner.start("probe") is StrategyStatus.RUNNING
    assert probe(runner).sma.value is None  # 2 history bars < period 3
    market.bars.publish(BarEvent(bar=bar(1, "1.3"), broker="acct"))  # overlap with history: not fed twice
    market.bars.publish(BarEvent(bar=bar(2, "2"), broker="acct"))
    market.bars.publish(BarEvent(bar=bar(3, "2", Timeframe.M5), broker="acct"))  # not subscribed
    market.bars.publish(BarEvent(bar=bar(4, "2"), broker="other-account"))  # other feed
    await settle()
    assert probe(runner).sma_at_bar[-1] == (D(1) + D("1.3") + D(2)) / 3  # updated BEFORE on_bar
    [(kind, intent)] = oms.calls
    assert kind == "submit"
    assert isinstance(intent, OrderIntent)
    assert (intent.strategy_id, intent.strategy_version, intent.side, intent.signal_ts) == (
        "probe",
        "7",
        Side.BUY,
        T0,
    )


async def test_a_raising_callback_is_rolled_back_and_repeated_errors_pause(env: Any) -> None:
    market, oms, _, runner, events, _ = env
    runner.add(spec(params={"explode_on": "1.5"}))
    await runner.start("probe")
    market.bars.publish(BarEvent(bar=bar(0, "1"), broker="acct"))
    await settle()
    assert runner.handles["probe"].ctx.state == {"bars": 1}
    market.bars.publish(BarEvent(bar=bar(1, "1.5"), broker="acct"))
    await settle()
    h = runner.handles["probe"]
    assert oms.calls == []  # the queued order was dropped
    assert h.ctx.state == {"bars": 1}  # the state change was rolled back
    assert (h.stats.errors, h.status) == (1, StrategyStatus.RUNNING)
    market.bars.publish(BarEvent(bar=bar(2, "1.5"), broker="acct"))
    await settle()
    assert h.status is StrategyStatus.PAUSED
    assert "2 errors" in (h.pause_reason or "")
    market.bars.publish(BarEvent(bar=bar(3, "2"), broker="acct"))
    await settle()
    assert oms.calls == []  # paused: no callbacks...
    assert probe(runner).sma.value is not None  # ...but indicators keep updating
    runner.resume("probe")
    assert (h.status, h.stats.errors) == (StrategyStatus.RUNNING, 0)
    await settle()
    assert [e.kind for e in events if e.kind in ("error", "paused", "resumed")] == [
        "error",
        "error",
        "paused",
        "resumed",
    ]


async def test_kill_switch_pauses_and_clearing_resumes_only_risk_paused(env: Any) -> None:
    _, oms, _, runner, _, _ = env
    runner.add(spec())
    runner.add(spec(id="manual"))
    await runner.start("probe")
    await runner.start("manual")
    runner.pause("manual", "operator")
    oms.events.publish(
        RiskStateEvent(
            account_id="acct",
            level=KillLevel.PAUSE_NEW,
            previous=KillLevel.NONE,
            reason="daily_loss",
            triggers=("daily_loss",),
            ts=T0,
        )
    )
    await settle()
    assert runner.handles["probe"].status is StrategyStatus.PAUSED
    assert runner.handles["probe"].paused_by_risk
    oms.level = KillLevel.PAUSE_NEW
    with pytest.raises(ValueError, match="kill switch"):
        runner.resume("probe")
    oms.level = KillLevel.NONE
    oms.events.publish(
        RiskStateEvent(
            account_id="acct",
            level=KillLevel.NONE,
            previous=KillLevel.PAUSE_NEW,
            reason="reset",
            triggers=(),
            ts=T0,
        )
    )
    await settle()
    assert runner.handles["probe"].status is StrategyStatus.RUNNING
    assert runner.handles["manual"].status is StrategyStatus.PAUSED  # paused by hand: stays paused


async def test_strategy_started_while_killed_starts_paused(env: Any) -> None:
    _, oms, _, runner, _, _ = env
    oms.level = KillLevel.FLATTEN_ALL
    runner.add(spec())
    assert await runner.start("probe") is StrategyStatus.PAUSED


async def test_only_own_orders_and_positions_are_routed_and_touchable(env: Any) -> None:
    _, oms, _, runner, _, _ = env
    runner.add(spec())
    await runner.start("probe")
    mine = Position(
        position_id="P1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.1"),
        entry_price=D("1.1"),
        opened_ts=T0,
        strategy_id="probe",
    )
    theirs = Position(
        position_id="P2",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.1"),
        entry_price=D("1.1"),
        opened_ts=T0,
        strategy_id="other",
    )
    oms.positions = (mine, theirs)
    for p in (mine, theirs):
        oms.events.publish(PositionEvent(account_id="acct", position=p, change=PositionChange.OPENED, ts=T0))
    await settle()
    assert ("position", "P1") in probe(runner).seen
    assert ("position", "P2") not in probe(runner).seen
    ctx = runner.handles["probe"].ctx
    ctx.close(mine)
    ctx.close(theirs)
    runner._execute(runner.handles["probe"], ctx.take_commands())
    await settle()
    assert oms.calls == [("close", "P1")]
    assert runner.handles["probe"].stats.rejected_commands == 1


async def test_timers_fire_on_the_strategy_clock(env: Any) -> None:
    _, _, clock, runner, _, _ = env
    runner.add(spec())
    await runner.start("probe")
    ctx = runner.handles["probe"].ctx
    ctx.set_timer("hourly", every=timedelta(hours=1))
    ctx.set_timer("once", at=T0 + timedelta(minutes=30))
    clock.advance(timedelta(minutes=31))
    await asyncio.sleep(0.03)
    clock.advance(timedelta(hours=3))  # missed periods fire once
    await asyncio.sleep(0.03)
    timers = [x for k, x in probe(runner).seen if k == "timer"]
    assert timers == ["once", "hourly"]
    assert ctx.timers["hourly"].next_at > clock.now()


async def test_state_is_persisted_restored_and_carried_over_by_replace(env: Any) -> None:
    market, _, _, runner, _, tmp_path = env
    runner.add(spec())
    await runner.start("probe")
    market.bars.publish(BarEvent(bar=bar(0, "1"), broker="acct"))
    await settle()
    await runner.stop("probe")
    assert json.loads((tmp_path / "probe.json").read_text()) == {"bars": 1}
    assert ("stop", None) in probe(runner).seen
    runner.add(spec(params={"period": 5}))
    await runner.start("probe")
    assert probe(runner).seen[0] == ("start", {"bars": 1})  # restored before on_start
    await runner.replace("probe", {"period": 2})
    assert probe(runner).params.period == 2  # type: ignore[attr-defined]
    assert probe(runner).seen[0] == ("start", {"bars": 1})
    with pytest.raises(ValueError, match="period"):
        await runner.replace("probe", {"period": "x"})
    assert runner.handles["probe"].status is StrategyStatus.RUNNING  # bad params: old instance untouched


async def test_non_json_state_is_an_error_and_unknown_params_are_rejected(env: Any) -> None:
    market, _, _, runner, _, _ = env

    class Bad(Probe):
        def on_bar(self, ctx: StrategyContext, bar: Bar) -> None:
            ctx.state["when"] = bar.start  # type: ignore[assignment]

    runner.add(spec(cls=Bad))
    await runner.start("probe")
    market.bars.publish(BarEvent(bar=bar(0, "1"), broker="acct"))
    await settle()
    h = runner.handles["probe"]
    assert h.stats.errors == 1
    assert h.ctx.state == {}
    with pytest.raises(ValueError, match="extra"):
        runner.add(spec(id="x", params={"nope": 1}))


async def test_on_order_and_ticks_are_routed(env: Any) -> None:
    market, oms, _, runner, _, _ = env
    runner.add(spec())
    await runner.start("probe")
    intent = OrderIntent(
        symbol="EURUSD",
        side=Side.BUY,
        order_type=OrderType.MARKET,
        volume=D("0.1"),
        strategy_id="probe",
        strategy_version="7",
        signal_ts=T0,
    )
    order = Order.new(intent.to_request("01J00000000000000000000001"), T0)
    oms.events.publish(OrderEvent(account_id="acct", order=order, previous_status=None, ts=T0))
    market.ticks.publish(
        TickEvent(
            tick=Tick(symbol="EURUSD", bid=D("1.1"), ask=D("1.1001"), ts=T0, recv_ts=T0, recv_mono_ns=0),
            broker="acct",
        )
    )
    await settle()
    assert ("order", OrderStatus.NEW) in probe(runner).seen
    assert runner.handles["probe"].ctx.last_tick("EURUSD") is not None
    with pytest.raises(ValueError, match="not subscribed"):
        runner.handles["probe"].ctx.last_tick("GBPUSD")
