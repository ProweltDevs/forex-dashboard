"""StrategyRunner: lifecycle, event routing, isolation and state persistence for strategy instances
(SPEC §8, ADR 0009).

- Lifecycle: LOADED -> WARMING (state restored, indicators warmed, ``on_start``) -> RUNNING <-> PAUSED ->
  STOPPED. ``start``/``pause``/``resume``/``stop``/``replace`` work while the process runs (the future
  control API calls them).
- Routing: complete bars and ticks of the strategy's account feed and subscriptions; ``on_order`` /
  ``on_position`` for its own orders and positions only.
- Isolation: a raising callback is rolled back (queued commands dropped, ``ctx.state`` restored), logged and
  counted; ``max_errors`` errors pause that strategy only. Callbacks slower than the budget are logged.
- Kill switch: at PAUSE_NEW and above the account's running strategies pause; they resume when the level
  returns to NONE (strategies paused by hand or by errors stay paused).
- Commands go through the account's OrderManager (so through risk). A strategy may only modify, cancel or
  close its own orders and positions.
- ``ctx.state`` is saved (off the event loop) after every callback that changed it and restored on start.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from trader.brokers.base import BrokerAck, OrderChanges
from trader.domain.enums import KillLevel, Timeframe
from trader.domain.errors import TraderError
from trader.domain.events import (
    BarEvent,
    DomainEvent,
    OrderEvent,
    PositionEvent,
    RiskStateEvent,
    StrategyEvent,
    TickEvent,
)
from trader.domain.jsontypes import JsonValue
from trader.domain.models import Bar, Order, OrderIntent, Tick
from trader.domain.time import Clock
from trader.marketdata.bus import Bus, Subscription, TickBus
from trader.observability.logging import get_logger
from trader.persistence.files import read_text, write_atomic
from trader.risk.gate import AccountView

from .base import Strategy, StrategyStatus
from .context import CancelOrder, ClosePosition, Command, ModifyOrder, ModifyPosition, StrategyContext, Submit

_log = get_logger("trader.strategy.runner")


class OrderPort(Protocol):
    """The slice of OrderManager the runner uses."""

    events: Bus[DomainEvent]

    @property
    def risk_level(self) -> KillLevel: ...
    def view(self) -> AccountView: ...
    async def submit(self, intent: OrderIntent) -> Order: ...
    async def modify_order(self, client_order_id: str, changes: OrderChanges) -> BrokerAck | None: ...
    async def cancel_order(self, client_order_id: str, reason: str = ...) -> BrokerAck | None: ...
    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck: ...
    async def modify_position(
        self, position_id: str, sl: Decimal | None, tp: Decimal | None
    ) -> BrokerAck | None: ...


class MarketPort(Protocol):
    """The slice of MarketDataService the runner uses. ``feed`` = the account id."""

    ticks: TickBus
    bars: Bus[BarEvent]

    def history(self, broker: str, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]: ...
    def last_tick(self, symbol: str, broker: str | None = None) -> Tick | None: ...


class StrategyStateStore:
    """``{dir}/{strategy_id}.json``: the strategy's ``ctx.state`` (atomic writes, 0600)."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path(self, strategy_id: str) -> Path:
        return self.directory / f"{strategy_id}.json"

    def load(self, strategy_id: str) -> dict[str, JsonValue] | None:
        raw = read_text(self.path(strategy_id))
        if raw is None:
            return None
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(f"strategy state for {strategy_id} is not a JSON object")
        return data

    def save(self, strategy_id: str, text: str) -> None:
        write_atomic(self.path(strategy_id), text)


@dataclass(frozen=True, slots=True, kw_only=True)
class StrategySpec:
    id: str
    cls: type[Strategy[Any]]
    params: Mapping[str, object]
    account: str  # account id: also the market-data feed key
    symbols: tuple[str, ...]
    timeframes: tuple[Timeframe, ...]
    max_errors: int = 5
    callback_budget_ms: float = 50.0
    seed: int = 0


@dataclass(slots=True)
class StrategyStats:
    callbacks: int = 0
    errors: int = 0  # since the last (re)start or resume
    total_errors: int = 0
    slow: int = 0
    intents: int = 0
    commands: int = 0
    rejected_commands: int = 0
    last_error: str | None = None


@dataclass(slots=True)
class StrategyHandle:
    spec: StrategySpec
    strategy: Strategy[Any]
    ctx: StrategyContext
    status: StrategyStatus = StrategyStatus.LOADED
    pause_reason: str | None = None
    paused_by_risk: bool = False
    saved_state: str = "{}"
    stats: StrategyStats = field(default_factory=StrategyStats)


class _Env:
    """StrategyEnv over the live services for one account."""

    def __init__(self, market: MarketPort, oms: OrderPort, feed: str, clock: Clock) -> None:
        self._market, self._oms, self._feed, self._clock = market, oms, feed, clock

    def now(self) -> datetime:
        return self._clock.now()

    def bars(self, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        return self._market.history(self._feed, symbol, tf, n)

    def last_tick(self, symbol: str) -> Tick | None:
        return self._market.last_tick(symbol, self._feed)

    def view(self) -> AccountView:
        return self._oms.view()


class StrategyRunner:
    def __init__(
        self,
        market: MarketPort,
        accounts: Mapping[str, OrderPort],
        clock: Clock,
        store: StrategyStateStore | None = None,
        *,
        timer_interval: timedelta = timedelta(milliseconds=250),
    ) -> None:
        self._market = market
        self._accounts = dict(accounts)
        self._clock = clock
        self._store = store
        self._timer_interval = timer_interval
        self.handles: dict[str, StrategyHandle] = {}
        self.events: Bus[StrategyEvent] = Bus()
        self._tasks: list[asyncio.Task[None]] = []
        self._spawned: set[asyncio.Task[None]] = set()
        self._dirty: dict[str, str] = {}
        self._dirty_event = asyncio.Event()
        self._write_lock = asyncio.Lock()

    # ================================================================== lifecycle

    def add(self, spec: StrategySpec) -> StrategyHandle:
        """Validate params and create the instance (LOADED). Raises on bad params or a duplicate id."""
        if spec.id in self.handles and self.handles[spec.id].status is not StrategyStatus.STOPPED:
            raise ValueError(f"strategy {spec.id!r} is already loaded")
        oms = self._accounts.get(spec.account)
        if oms is None:
            raise ValueError(f"strategy {spec.id!r}: unknown account {spec.account!r}")
        params = spec.cls.Params.model_validate(dict(spec.params))
        strategy = spec.cls(spec.id, params)
        ctx = StrategyContext(
            strategy_id=spec.id,
            version=spec.cls.version,
            symbols=spec.symbols,
            timeframes=spec.timeframes,
            params=params,
            env=_Env(self._market, oms, spec.account, self._clock),
            state={},
            seed=spec.seed,
        )
        handle = StrategyHandle(spec=spec, strategy=strategy, ctx=ctx)
        self.handles[spec.id] = handle
        self._event(spec.id, "loaded", f"{spec.cls.__name__} v{spec.cls.version} on {spec.account}")
        return handle

    async def start(self, strategy_id: str) -> StrategyStatus:
        handle = self.handles[strategy_id]
        if handle.status is not StrategyStatus.LOADED:
            raise ValueError(f"{strategy_id} is {handle.status}, not LOADED")
        handle.status = StrategyStatus.WARMING
        state = await asyncio.to_thread(self._store.load, strategy_id) if self._store else None
        handle.ctx.state = state or {}
        handle.saved_state = _dump(handle.ctx.state)
        handle.status = StrategyStatus.RUNNING  # on_start runs as a normal (isolated) callback
        ok = self._invoke(handle, "on_start", lambda: handle.strategy.on_start(handle.ctx), force=True)
        if not ok:
            handle.status = StrategyStatus.PAUSED
            handle.pause_reason = "on_start failed"
            self._event(strategy_id, "paused", "on_start failed")
            return handle.status
        level = self._accounts[handle.spec.account].risk_level
        if level is not KillLevel.NONE:
            self._pause(handle, f"kill switch {level}", by_risk=True)
        else:
            self._event(strategy_id, "started", "running")
        return handle.status

    def pause(self, strategy_id: str, reason: str = "requested") -> None:
        handle = self.handles[strategy_id]
        if handle.status is StrategyStatus.RUNNING:
            self._pause(handle, reason, by_risk=False)

    def resume(self, strategy_id: str) -> None:
        handle = self.handles[strategy_id]
        if handle.status is not StrategyStatus.PAUSED:
            return
        level = self._accounts[handle.spec.account].risk_level
        if level is not KillLevel.NONE:
            raise ValueError(f"{strategy_id}: account kill switch is {level}; reset it first")
        handle.status, handle.pause_reason, handle.paused_by_risk = StrategyStatus.RUNNING, None, False
        handle.stats.errors = 0
        self._event(strategy_id, "resumed", "running")

    async def stop(self, strategy_id: str) -> None:
        handle = self.handles[strategy_id]
        if handle.status in (StrategyStatus.STOPPED, StrategyStatus.LOADED):
            handle.status = StrategyStatus.STOPPED
            return
        self._invoke(handle, "on_stop", lambda: handle.strategy.on_stop(handle.ctx), force=True)
        handle.status = StrategyStatus.STOPPED
        await self._flush()
        self._event(strategy_id, "stopped", "stopped")

    async def replace(self, strategy_id: str, params: Mapping[str, object]) -> StrategyStatus:
        """New params = new instance: the old one stops cleanly first (state carries over)."""
        old = self.handles[strategy_id]
        spec = StrategySpec(**{**_spec_fields(old.spec), "params": params})
        spec.cls.Params.model_validate(dict(params))  # fail before stopping the old one
        await self.stop(strategy_id)
        self.add(spec)
        return await self.start(strategy_id)

    async def start_all(self) -> None:
        for sid, handle in list(self.handles.items()):
            if handle.status is StrategyStatus.LOADED:
                await self.start(sid)

    async def stop_all(self) -> None:
        for sid in list(self.handles):
            await self.stop(sid)

    # ================================================================== running

    async def run(self) -> None:
        """Start the consumer tasks (bars, ticks, OMS events per account, timers, state writer)."""
        if self._tasks:
            return
        bars = self._market.bars.subscribe("strategies-bars")
        ticks = self._market.ticks.subscribe("strategies-ticks")
        self._tasks.append(asyncio.create_task(self._consume(bars, self._on_bar_event), name="strategy-bars"))
        self._tasks.append(
            asyncio.create_task(self._consume(ticks, self._on_tick_event), name="strategy-ticks")
        )
        for account, oms in self._accounts.items():
            sub = oms.events.subscribe(f"strategies-{account}")
            self._tasks.append(
                asyncio.create_task(
                    self._consume(sub, partial(self._on_oms_event, account)),
                    name=f"strategy-oms-{account}",
                )
            )
        self._tasks.append(asyncio.create_task(self._timers(), name="strategy-timers"))
        self._tasks.append(asyncio.create_task(self._writer(), name="strategy-state-writer"))

    async def close(self) -> None:
        """Stop the consumer tasks (call after ``stop_all``); pending state is flushed."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if self._spawned:
            await asyncio.wait(self._spawned, timeout=10)
        await self._flush()
        self.events.close()

    async def _consume[T](self, sub: Subscription[T], handler: Callable[[T], None]) -> None:
        async for item in sub:
            try:
                handler(item)
            except Exception:  # a runner bug must not stop the feed for every strategy
                _log.exception("strategy_dispatch_failed")

    def _on_bar_event(self, event: BarEvent) -> None:
        bar = event.bar
        for h in self._subscribed(event.broker, bar.symbol, bar.timeframe):
            h.ctx.feed_bar(bar)  # indicators stay current while paused
            if h.status is StrategyStatus.RUNNING:
                self._invoke(h, "on_bar", partial(h.strategy.on_bar, h.ctx, bar))

    def _on_tick_event(self, event: TickEvent) -> None:
        tick = event.tick
        for h in self._subscribed(event.broker, tick.symbol, None):
            if h.status is StrategyStatus.RUNNING:
                self._invoke(h, "on_tick", partial(h.strategy.on_tick, h.ctx, tick))

    def _on_oms_event(self, account: str, event: DomainEvent) -> None:
        if isinstance(event, RiskStateEvent):
            self._on_risk(account, event)
            return
        if isinstance(event, OrderEvent):
            h = self._owner(account, event.order.request.strategy_id)
            if h is not None and h.status is StrategyStatus.RUNNING:
                self._invoke(h, "on_order", partial(h.strategy.on_order, h.ctx, event))
        elif isinstance(event, PositionEvent):
            h = self._owner(account, event.position.strategy_id)
            if h is not None and h.status is StrategyStatus.RUNNING:
                self._invoke(h, "on_position", partial(h.strategy.on_position, h.ctx, event))

    def _on_risk(self, account: str, event: RiskStateEvent) -> None:
        for h in self.handles.values():
            if h.spec.account != account:
                continue
            if event.level is not KillLevel.NONE and h.status is StrategyStatus.RUNNING:
                self._pause(h, f"kill switch {event.level}: {event.reason}", by_risk=True)
            elif event.level is KillLevel.NONE and h.status is StrategyStatus.PAUSED and h.paused_by_risk:
                h.status, h.pause_reason, h.paused_by_risk = StrategyStatus.RUNNING, None, False
                self._event(h.spec.id, "resumed", "kill switch cleared")

    async def _timers(self) -> None:
        while True:
            await asyncio.sleep(self._timer_interval.total_seconds())
            now = self._clock.now()
            for h in list(self.handles.values()):
                if h.status is not StrategyStatus.RUNNING:
                    continue
                for name in h.ctx.due_timers(now):
                    self._invoke(h, "on_timer", partial(h.strategy.on_timer, h.ctx, name))

    def _subscribed(self, feed: str, symbol: str, tf: Timeframe | None) -> list[StrategyHandle]:
        return [
            h
            for h in self.handles.values()
            if h.spec.account == feed
            and symbol in h.spec.symbols
            and (tf is None or tf in h.spec.timeframes)
            and h.status in (StrategyStatus.RUNNING, StrategyStatus.PAUSED)
        ]

    def _owner(self, account: str, strategy_id: str) -> StrategyHandle | None:
        h = self.handles.get(strategy_id)
        return h if h is not None and h.spec.account == account else None

    # ================================================================== isolation

    def _invoke(self, h: StrategyHandle, name: str, fn: Callable[[], None], *, force: bool = False) -> bool:
        """Run one callback; returns False if it raised (and was rolled back)."""
        if not force and h.status is not StrategyStatus.RUNNING:
            return False
        h.stats.callbacks += 1
        started = time.perf_counter()
        try:
            fn()
            state = _dump(h.ctx.state)  # non-JSON state is a strategy error too
        except Exception as exc:
            h.ctx.take_commands()
            h.ctx.state = json.loads(h.saved_state)
            self._error(h, name, exc)
            return False
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            if elapsed_ms > h.spec.callback_budget_ms:
                h.stats.slow += 1
                _log.warning(
                    "strategy_slow_callback", strategy=h.spec.id, callback=name, ms=round(elapsed_ms, 1)
                )
                self._event(h.spec.id, "slow_callback", f"{name} took {elapsed_ms:.1f} ms")
        if state != h.saved_state:
            h.saved_state = state
            self._dirty[h.spec.id] = state
            self._dirty_event.set()
        self._execute(h, h.ctx.take_commands())
        return True

    def _error(self, h: StrategyHandle, name: str, exc: BaseException) -> None:
        h.stats.errors += 1
        h.stats.total_errors += 1
        h.stats.last_error = f"{name}: {type(exc).__name__}: {exc}"
        _log.error("strategy_error", strategy=h.spec.id, callback=name, error=repr(exc), exc_info=exc)
        self._event(h.spec.id, "error", h.stats.last_error)
        if h.stats.errors >= h.spec.max_errors and h.status is StrategyStatus.RUNNING:
            self._pause(h, f"{h.stats.errors} errors (last: {h.stats.last_error})", by_risk=False)

    def _pause(self, h: StrategyHandle, reason: str, *, by_risk: bool) -> None:
        h.status, h.pause_reason, h.paused_by_risk = StrategyStatus.PAUSED, reason, by_risk
        _log.warning("strategy_paused", strategy=h.spec.id, reason=reason)
        self._event(h.spec.id, "paused", reason)

    # ================================================================== commands

    def _execute(self, h: StrategyHandle, commands: list[Command]) -> None:
        if not commands:
            return
        oms = self._accounts[h.spec.account]
        view = oms.view()
        mine_orders = {o.client_order_id for o in view.open_orders if o.request.strategy_id == h.spec.id}
        mine_positions = {p.position_id for p in view.positions if p.strategy_id == h.spec.id}
        for cmd in commands:
            h.stats.commands += 1
            call: Callable[[], Awaitable[object]]
            match cmd:
                case Submit(intent=intent):
                    h.stats.intents += 1
                    call = partial(oms.submit, intent)
                case ModifyOrder(client_order_id=cid, changes=changes) if cid in mine_orders:
                    call = partial(oms.modify_order, cid, changes)
                case CancelOrder(client_order_id=cid) if cid in mine_orders:
                    call = partial(oms.cancel_order, cid, f"strategy {h.spec.id}")
                case ClosePosition(position_id=pid, volume=volume) if pid in mine_positions:
                    call = partial(oms.close_position, pid, volume)
                case ModifyPosition(position_id=pid, sl=sl, tp=tp) if pid in mine_positions:
                    call = partial(oms.modify_position, pid, sl, tp)
                case _:
                    h.stats.rejected_commands += 1
                    self._error(
                        h, "command", ValueError(f"{type(cmd).__name__} on an order/position it does not own")
                    )
                    continue
            self._spawn(self._send(h, cmd, call))

    async def _send(self, h: StrategyHandle, cmd: Command, call: Callable[[], Awaitable[object]]) -> None:
        try:
            await call()
        except TraderError as exc:  # broker/OMS refusals are not strategy bugs; report them
            _log.warning(
                "strategy_command_failed", strategy=h.spec.id, command=type(cmd).__name__, error=str(exc)
            )
            self._event(h.spec.id, "command_failed", f"{type(cmd).__name__}: {exc}")

    def _spawn(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._spawned.add(task)
        task.add_done_callback(self._spawned.discard)

    # ================================================================== state + events

    async def _writer(self) -> None:
        while True:
            await self._dirty_event.wait()
            await self._flush()
            await asyncio.sleep(0.2)  # coalesce bursts (one write per strategy per 200 ms at most)

    async def _flush(self) -> None:
        """Write pending state. Serialised: a caller (e.g. ``stop``) also waits for a write in progress."""
        async with self._write_lock:
            self._dirty_event.clear()
            batch, self._dirty = self._dirty, {}
            if not batch or self._store is None:
                return
            store = self._store
            for sid, text in batch.items():
                try:
                    await asyncio.to_thread(store.save, sid, text)
                except OSError as exc:
                    _log.error("strategy_state_save_failed", strategy=sid, error=str(exc))
                    self._dirty.setdefault(sid, text)

    def _event(self, strategy_id: str, kind: str, message: str) -> None:
        self.events.publish(
            StrategyEvent(strategy_id=strategy_id, kind=kind, message=message, ts=self._clock.now())
        )

    def status(self) -> list[tuple[str, StrategyStatus, str | None, StrategyStats]]:
        return [(sid, h.status, h.pause_reason, h.stats) for sid, h in self.handles.items()]


def _dump(state: Mapping[str, JsonValue]) -> str:
    return json.dumps(state, sort_keys=True, separators=(",", ":"))


def _spec_fields(spec: StrategySpec) -> dict[str, Any]:
    return {name: getattr(spec, name) for name in StrategySpec.__dataclass_fields__}
