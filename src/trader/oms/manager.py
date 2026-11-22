"""OrderManager (SPEC §3, §6): the only writer of order state for one account.

Single writer: every mutation runs as a job on one task (``run``), in arrival order. Broker calls for new
orders run as separate tasks and post their outcome back as jobs, so a slow ACK never blocks event
processing. Reconcile and targeted lookups await the broker inside the writer: anything that arrives
meanwhile is applied afterwards, in order, and stale updates are ignored by status rank.

Submit path: intent -> RiskGate.pre_trade -> client_order_id (ULID) -> persisted as PENDING_SUBMIT -> broker.
- ACK -> ACCEPTED; results (WORKING/FILLED/...) come from broker events; fills from FillEvents.
- Timeout or ``UnknownOrderState`` -> UNKNOWN -> after a grace period, targeted reconcile
  (``broker.find_order``); if the broker has no such order, resubmit with the same id only while the signal
  is fresh (``resubmit_window``), else give up (REJECTED, ReconcileEvent, error log). Never assume failure.
- Retryable errors are retried with the same id (bounded); others reject.

Capability gaps are emulated: STOP_LIMIT, GTD/DAY expiry, SL/TP attach-after-fill, plus the strategy's
exits (trailing, break-even, time exit, scale-out) and OCO groups. State survives restarts via the store.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal

from trader.brokers.base import Broker, BrokerAck, OrderChanges
from trader.domain.enums import (
    ConnectionState,
    KillLevel,
    OrderStatus,
    OrderType,
    PositionChange,
    RiskDecision,
    TimeInForce,
)
from trader.domain.errors import (
    BrokerError,
    BrokerUnavailable,
    OrderNotFound,
    OrderRejected,
    PositionNotFound,
    RateLimited,
    TraderError,
    UnknownOrderState,
)
from trader.domain.events import (
    AccountEvent,
    ConnectionEvent,
    DomainEvent,
    FillEvent,
    OrderEvent,
    PositionEvent,
    ReconcileEvent,
    RiskEvent,
    RiskStateEvent,
)
from trader.domain.ids import new_client_order_id
from trader.domain.models import (
    AccountState,
    Fill,
    Instrument,
    Order,
    OrderIntent,
    OrderRequest,
    Position,
    Tick,
)
from trader.domain.money import ZERO
from trader.domain.time import Clock, ensure_utc, from_epoch_ms, to_epoch_ms
from trader.marketdata.bus import Bus
from trader.observability.logging import get_logger
from trader.risk.gate import AccountView, RiskGate, RiskOutput

from . import codec
from .emulation import Action, CancelOrder, ClosePosition, EmulationEngine, ModifyStop, SendStopLimit
from .state_machine import apply_fill, can_transition, transition
from .store import OmsState, OmsStore

_log = get_logger("trader.oms")

S = OrderStatus
_FILL_STATES = frozenset({S.PARTIALLY_FILLED, S.FILLED})
_OPEN = frozenset({S.NEW, S.PENDING_SUBMIT, S.UNKNOWN, S.ACCEPTED, S.WORKING, S.PARTIALLY_FILLED})
_RANK = {S.NEW: 0, S.PENDING_SUBMIT: 1, S.UNKNOWN: 1, S.ACCEPTED: 2, S.WORKING: 3, S.PARTIALLY_FILLED: 4}
MANUAL = "manual"


@dataclass(frozen=True, slots=True, kw_only=True)
class OmsSettings:
    account_id: str
    ack_timeout: timedelta = timedelta(seconds=10)
    unknown_grace: timedelta = timedelta(seconds=3)
    resubmit_window: timedelta = timedelta(seconds=30)  # never resubmit a signal older than this
    max_attempts: int = 3
    retry_backoff: timedelta = timedelta(milliseconds=500)
    reconcile_interval: timedelta = timedelta(minutes=5)
    timer_interval: timedelta = timedelta(seconds=1)
    keep_finished: int = 1_000
    keep_fill_ids: int = 20_000
    min_modify_interval: timedelta = timedelta(seconds=1)


@dataclass(slots=True)
class _Tracked:
    order: Order
    attempts: int = 0
    first_sent: datetime | None = None
    synthetic: Decimal = ZERO  # filled volume taken from a broker order view, awaiting the real fills
    position_id: str | None = None
    exits_armed: bool = False
    sltp_attached: bool = False
    oco_done: bool = False
    waiter: asyncio.Future[Order] | None = None
    absent_once: bool = False  # one "not found" lookup seen (non-idempotent brokers look twice)


type _Job = Callable[[], Awaitable[None]]


class OrderManager:
    def __init__(
        self,
        broker: Broker,
        risk: RiskGate,
        store: OmsStore,
        clock: Clock,
        settings: OmsSettings,
    ) -> None:
        self._broker = broker
        self._risk = risk
        self._store = store
        self._clock = clock
        self._s = settings
        self.events: Bus[DomainEvent] = Bus()
        self.ready = asyncio.Event()  # set after a successful reconcile; strategies wait on it
        self._inbox: asyncio.Queue[_Job | None] = asyncio.Queue()
        self._writer: asyncio.Task[None] | None = None
        self._timer: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._orders: dict[str, _Tracked] = {}
        self._by_broker_id: dict[str, str] = {}
        self._positions: dict[str, Position] = {}
        self._pos_strategy: dict[str, str] = {}
        self._fill_ids: dict[str, None] = {}
        self._account: AccountState | None = None
        self._instruments: dict[str, Instrument] = {}
        self._last_ticks: dict[str, Tick] = {}
        self._connected = True
        self._dirty = False
        self._last_reconcile: datetime | None = None
        self.emulation = EmulationEngine(min_modify_interval=settings.min_modify_interval)

    # ================================================================== lifecycle

    async def start(self) -> None:
        """Restore persisted state and start the writer. Call ``reconcile("startup")`` next."""
        if self._writer is not None:
            return
        state = await asyncio.to_thread(self._store.load)
        if state is not None:
            self._restore(state)
        self._writer = asyncio.create_task(self._run(), name=f"oms-{self._s.account_id}")
        self._timer = asyncio.create_task(self._tick_timer(), name=f"oms-timer-{self._s.account_id}")

    async def stop(self) -> None:
        """Graceful: drain queued jobs, persist, stop. In-flight broker calls are left to finish."""
        if self._timer is not None:
            self._timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer
        if self._writer is not None:
            self._inbox.put_nowait(None)
            await self._writer
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._persist()
        self._writer = self._timer = None
        self.events.close()

    async def abort(self) -> None:
        """Test hook: die like a crashed process (no drain, no final persist)."""
        for task in [t for t in (self._timer, self._writer, *self._tasks) if t is not None]:
            task.cancel()
        await asyncio.gather(
            *[t for t in (self._timer, self._writer, *self._tasks) if t], return_exceptions=True
        )
        self._writer = self._timer = None

    async def _run(self) -> None:
        while (job := await self._inbox.get()) is not None:
            try:
                await job()
            except Exception:
                _log.exception("oms_job_failed", account=self._s.account_id)
            if self._dirty:
                await self._persist()

    def _post(self, job: _Job) -> None:
        self._inbox.put_nowait(job)

    async def flush(self) -> None:
        """Wait until every job queued so far (events, commands) has been applied."""

        async def barrier() -> None:
            return None

        await self._call(barrier)

    async def _call[T](self, job: Callable[[], Awaitable[T]]) -> T:
        """Run ``job`` on the writer and wait for its result."""
        fut: asyncio.Future[T] = asyncio.get_running_loop().create_future()

        async def wrapper() -> None:
            try:
                fut.set_result(await job())
            except BaseException as exc:
                fut.set_exception(exc)

        self._post(wrapper)
        return await fut

    def _spawn(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _later(self, delay: timedelta, job: _Job) -> None:
        async def wait_then_post() -> None:
            await asyncio.sleep(max(0.0, delay.total_seconds()))
            self._post(job)

        self._spawn(wait_then_post())

    async def _tick_timer(self) -> None:
        while True:
            await asyncio.sleep(self._s.timer_interval.total_seconds())
            self._post(self._on_timer)

    async def _on_timer(self) -> None:
        now = self._now()
        await self._execute(self.emulation.on_time(now))
        self._apply_risk(self._risk.on_time(self.view()))
        if (
            self._connected
            and self.ready.is_set()
            and self._last_reconcile is not None
            and now - self._last_reconcile >= self._s.reconcile_interval
        ):
            await self._reconcile("periodic")

    def _now(self) -> datetime:
        return self._clock.now()

    def _publish(self, event: DomainEvent) -> None:
        self.events.publish(event)
        if not isinstance(event, RiskEvent | RiskStateEvent):
            self._apply_risk(self._risk.observe(event, self.view()))

    def _apply_risk(self, out: RiskOutput) -> None:
        """Act on a risk hook's output (on the writer). Cancels and closes are fire-and-forget; the engine
        re-asserts them while anything is left."""
        if out.changed:
            self._dirty = True
        for event in out.events:
            self.events.publish(event)
        if out.cancel_pending:
            for tracked in list(self._orders.values()):
                if tracked.order.status in (S.ACCEPTED, S.WORKING, S.PARTIALLY_FILLED):
                    self._spawn_cancel(tracked, f"kill switch {self._risk.level}")
        if out.flatten:
            for pos in list(self._positions.values()):
                self._spawn_close(pos.position_id, None, f"kill switch {self._risk.level}")

    # ================================================================== persistence

    def _state(self) -> OmsState:
        open_ = [t for t in self._orders.values() if t.order.status in _OPEN]
        done = sorted(
            (t for t in self._orders.values() if t.order.status not in _OPEN),
            key=lambda t: t.order.updated_ts,
        )
        keep = open_ + done[-self._s.keep_finished :]
        orders = []
        for t in keep:
            data = codec.order_to_json(t.order)
            data.update(
                attempts=t.attempts,
                first_sent_ms=to_epoch_ms(t.first_sent) if t.first_sent else None,
                synthetic=str(t.synthetic),
                position_id=t.position_id,
                exits_armed=t.exits_armed,
                sltp_attached=t.sltp_attached,
                oco_done=t.oco_done,
            )
            orders.append(data)
        return OmsState(
            orders=orders,
            position_strategy=dict(self._pos_strategy),
            fill_ids=list(self._fill_ids)[-self._s.keep_fill_ids :],
            emulations=self.emulation.dump(),
            positions=[codec.position_to_json(p) for p in self._positions.values()],
            risk=self._risk.dump(),
        )

    async def _persist(self) -> None:
        state = self._state()
        self._dirty = False
        await asyncio.to_thread(self._store.save, state)

    def _restore(self, state: OmsState) -> None:
        for data in state.orders:
            order = codec.order_from_json(data)
            first = data.get("first_sent_ms")
            pid = data.get("position_id")
            tracked = _Tracked(
                order=order,
                attempts=int(str(data.get("attempts") or 0)),
                first_sent=from_epoch_ms(int(str(first))) if first is not None else None,
                synthetic=Decimal(str(data.get("synthetic") or "0")),
                position_id=None if pid is None else str(pid),
                exits_armed=bool(data.get("exits_armed")),
                sltp_attached=bool(data.get("sltp_attached")),
                oco_done=bool(data.get("oco_done")),
            )
            self._orders[order.client_order_id] = tracked
            if order.broker_order_id:
                self._by_broker_id[order.broker_order_id] = order.client_order_id
        self._pos_strategy = dict(state.position_strategy)
        self._fill_ids = dict.fromkeys(state.fill_ids)
        self._positions = {p["position_id"]: codec.position_from_json(p) for p in state.positions}  # type: ignore[misc]
        self.emulation.load(state.emulations)
        self._risk.load(state.risk)
        _log.info(
            "oms_restored",
            account=self._s.account_id,
            orders=len(self._orders),
            emulations=len(self.emulation),
        )

    # ================================================================== queries

    def set_instruments(self, instruments: Iterable[Instrument]) -> None:
        self._instruments.update({i.symbol: i for i in instruments})

    def view(self) -> AccountView:
        return AccountView(
            account_id=self._s.account_id,
            account=self._account,
            positions=tuple(self._positions.values()),
            open_orders=tuple(t.order for t in self._orders.values() if t.order.status in _OPEN),
            instruments=dict(self._instruments),
            last_ticks=dict(self._last_ticks),
            broker_connected=self._connected,
        )

    def order(self, client_order_id: str) -> Order | None:
        t = self._orders.get(client_order_id)
        return t.order if t else None

    def orders(self) -> list[Order]:
        return [t.order for t in self._orders.values()]

    def positions(self) -> list[Position]:
        return list(self._positions.values())

    # ================================================================== public commands

    async def submit(self, intent: OrderIntent) -> Order:
        """Risk-check and send a new order. Returns once the broker ACKed (ACCEPTED), the order was
        rejected, or its outcome became UNKNOWN (resolution continues in the background)."""
        if not self.ready.is_set():
            raise BrokerUnavailable("account not reconciled yet; refusing new orders")
        fut: asyncio.Future[Order] = asyncio.get_running_loop().create_future()
        self._post(lambda: self._do_submit(intent, fut))
        return await fut

    async def modify_order(self, client_order_id: str, changes: OrderChanges) -> BrokerAck | None:
        return await self._call(lambda: self._do_modify(client_order_id, changes))

    async def cancel_order(self, client_order_id: str, reason: str = "cancel requested") -> BrokerAck | None:
        return await self._call(lambda: self._do_cancel(client_order_id, reason))

    async def modify_position(
        self, position_id: str, sl: Decimal | None, tp: Decimal | None
    ) -> BrokerAck | None:
        return await self._call(lambda: self._do_modify_position(position_id, sl, tp))

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        """Risk-reducing: allowed even while new entries are blocked."""
        return await self._call(lambda: self._do_close(position_id, volume))

    async def close_all(
        self, *, symbol: str | None = None, strategy_id: str | None = None, cancel_pending: bool = True
    ) -> int:
        """Close matching positions (and cancel matching pending orders). Returns requests sent."""
        return await self._call(lambda: self._do_close_all(symbol, strategy_id, cancel_pending))

    async def flatten(self) -> int:
        """Cancel every pending order and close every position of the account."""
        return await self.close_all()

    @property
    def risk_level(self) -> KillLevel:
        return self._risk.level

    async def kill_switch(self, level: KillLevel, reason: str) -> KillLevel:
        """Manual kill switch (API/CLI): PAUSE_NEW blocks entries, CANCEL_PENDING also cancels pending
        orders, FLATTEN_ALL also closes every position. Stays on until ``reset_kill_switch``."""

        async def job() -> KillLevel:
            self._apply_risk(self._risk.kill(level, reason, self.view()))
            return self._risk.level

        return await self._call(job)

    async def reset_kill_switch(self, reason: str) -> KillLevel:
        """Clear the manual switch and latched breakers. Conditions that still hold trigger again."""

        async def job() -> KillLevel:
            self._apply_risk(self._risk.reset(reason, self.view()))
            return self._risk.level

        return await self._call(job)

    async def reconcile(self, reason: str = "requested") -> bool:
        return await self._call(lambda: self._reconcile(reason))

    # ================================================================== broker input (from the account pump)

    def on_broker_event(self, event: DomainEvent) -> None:
        self._post(lambda: self._apply_event(event))

    def on_tick(self, tick: Tick) -> None:
        self._last_ticks[tick.symbol] = tick
        self._risk.on_tick(tick)
        if tick.symbol in self.emulation.symbols():
            self._post(lambda: self._emulate_tick(tick))

    # ================================================================== submit

    def _wire_request(self, req: OrderRequest, *, triggered_stop_limit: bool = False) -> OrderRequest:
        """What the broker gets: capability gaps are removed here and emulated by the OMS."""
        caps = self._broker.capabilities
        wire = req
        if triggered_stop_limit:
            wire = self._stop_limit_leg(req)
        if wire.time_in_force in (TimeInForce.GTD, TimeInForce.DAY) and not caps.server_side_expiry:
            wire = replace(wire, time_in_force=TimeInForce.GTC, expiry=None)
        if (wire.sl is not None or wire.tp is not None) and not caps.native_sl_tp:
            wire = replace(wire, sl=None, tp=None)
        return replace(wire, exits=(), oco_group=None)

    def _stop_limit_leg(self, req: OrderRequest) -> OrderRequest:
        """The order sent when an emulated STOP_LIMIT triggers. If the limit is still away from the market it
        rests as a LIMIT; if the market is already through it (brokers reject marketable limits) it goes as
        MARKET capped at the limit price via ``max_slippage``."""
        assert req.limit_price is not None
        tick = self._last_ticks.get(req.symbol)
        limit = req.limit_price
        if tick is not None:
            room = limit - tick.ask if req.side.sign > 0 else tick.bid - limit
            if room >= 0:
                return replace(
                    req,
                    order_type=OrderType.MARKET,
                    limit_price=None,
                    stop_price=None,
                    max_slippage=room,
                    time_in_force=TimeInForce.GTC,
                    expiry=None,
                )
        return replace(req, order_type=OrderType.LIMIT, stop_price=None)

    async def _do_submit(self, intent: OrderIntent, fut: asyncio.Future[Order]) -> None:
        now = self._now()
        verdict = self._risk.pre_trade(intent, self.view())
        cid = new_client_order_id()
        self._publish(
            RiskEvent(
                account_id=self._s.account_id,
                decision=RiskDecision.APPROVED if verdict.approved else RiskDecision.REJECTED,
                rule=verdict.rule,
                reason=verdict.reason,
                ts=now,
                strategy_id=intent.strategy_id,
                client_order_id=cid,
            )
        )
        request = intent.to_request(cid, volume=verdict.volume or intent.volume)
        tracked = _Tracked(order=Order.new(request, now))
        self._orders[cid] = tracked
        caps = self._broker.capabilities
        emulate_stop_limit = (
            request.order_type is OrderType.STOP_LIMIT and OrderType.STOP_LIMIT not in caps.order_types
        )

        def reject(reason: str) -> None:
            tracked.order = transition(tracked.order, S.REJECTED, now, reason=reason)
            self._order_changed(tracked)
            fut.set_result(tracked.order)

        if not verdict.approved:
            reject(f"risk:{verdict.rule}: {verdict.reason}")
            return
        if request.order_type not in caps.order_types and not emulate_stop_limit:
            reject(f"{request.order_type} is not supported by {self._broker.name}")
            return
        if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY) and not caps.server_side_expiry:
            deadline = request.expiry or (
                now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            )
            self.emulation.arm_expiry(client_order_id=cid, symbol=request.symbol, deadline=deadline)
        if emulate_stop_limit:
            assert request.stop_price is not None
            for status in (S.PENDING_SUBMIT, S.ACCEPTED, S.WORKING):
                tracked.order = transition(tracked.order, status, now, reason="emulated stop-limit")
            self.emulation.arm_stop_limit(
                client_order_id=cid, symbol=request.symbol, side=request.side, stop=request.stop_price
            )
            self._order_changed(tracked)
            fut.set_result(tracked.order)
            return
        tracked.order = transition(tracked.order, S.PENDING_SUBMIT, now)
        tracked.first_sent, tracked.attempts, tracked.waiter = now, 1, fut
        self._order_changed(tracked)
        await self._persist()  # durable BEFORE the broker sees it: a crash now is recoverable
        self._spawn(self._send(cid))

    async def _send(self, cid: str, *, triggered_stop_limit: bool = False) -> None:
        tracked = self._orders[cid]
        wire = self._wire_request(tracked.order.request, triggered_stop_limit=triggered_stop_limit)
        error: TraderError | None = None
        try:
            ack = await asyncio.wait_for(self._broker.place_order(wire), self._s.ack_timeout.total_seconds())
        except TimeoutError:
            error = UnknownOrderState("no ACK within ack_timeout")
        except BrokerError as exc:
            error = exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # anything else after the request may have left: outcome unknown
            error = UnknownOrderState(f"submit failed unexpectedly: {exc!r}")
        if error is not None:
            failure = error
            self._post(lambda: self._on_submit_error(cid, failure))
        else:
            self._post(lambda: self._on_ack(cid, ack))

    async def _on_ack(self, cid: str, ack: BrokerAck) -> None:
        tracked = self._orders[cid]
        if ack.broker_order_id:
            self._link_broker_id(tracked, ack.broker_order_id)
        if tracked.order.status in (S.PENDING_SUBMIT, S.UNKNOWN):
            self._advance(tracked, S.ACCEPTED, "broker ACK")
        self._order_changed(tracked)

    async def _on_submit_error(self, cid: str, exc: TraderError) -> None:
        tracked = self._orders[cid]
        order = tracked.order
        if order.status not in (S.PENDING_SUBMIT, S.UNKNOWN, S.WORKING):
            return  # events already resolved it
        now = self._now()
        if isinstance(exc, UnknownOrderState):
            if order.status is S.PENDING_SUBMIT:
                self._advance(tracked, S.UNKNOWN, str(exc))
            self._order_changed(tracked)
            self._later(self._s.unknown_grace, lambda: self._resolve(cid, "submit outcome unknown"))
            return
        retryable = isinstance(exc, BrokerError) and exc.retryable
        if retryable and tracked.attempts < self._s.max_attempts and self._fresh(order, now):
            delay = self._s.retry_backoff * tracked.attempts
            if isinstance(exc, RateLimited) and exc.retry_after_ms:
                delay = max(delay, timedelta(milliseconds=exc.retry_after_ms))
            _log.warning("oms_submit_retry", cid=cid, attempt=tracked.attempts, error=repr(exc))
            self._later(delay, lambda: self._resend(cid))
            return
        self._reject(tracked, f"{type(exc).__name__}: {exc}")

    def _fresh(self, order: Order, now: datetime) -> bool:
        born = order.request.signal_ts or order.history[0].ts
        return now - born <= self._s.resubmit_window

    async def _resend(self, cid: str) -> None:
        tracked = self._orders[cid]
        if tracked.order.status not in (S.PENDING_SUBMIT, S.UNKNOWN):
            return
        tracked.attempts += 1
        await self._persist()
        self._spawn(self._send(cid))

    def _reject(self, tracked: _Tracked, reason: str) -> None:
        if tracked.order.status.is_terminal:
            return
        tracked.order = transition(tracked.order, S.REJECTED, self._ts(tracked.order), reason=reason)
        self._order_changed(tracked)

    # ================================================================== targeted reconcile

    async def _resolve(self, cid: str, why: str) -> None:
        tracked = self._orders.get(cid)
        if tracked is None or tracked.order.status not in _OPEN:
            return
        stop_limit = self.emulation.stop_limit(cid)
        if stop_limit is not None and not stop_limit.triggered:
            return  # still local-only: nothing at the broker to find
        order = tracked.order
        since = (tracked.first_sent or order.history[0].ts) - timedelta(minutes=5)
        try:
            found = await self._broker.find_order(cid, since)
        except BrokerError as exc:
            _log.warning("oms_resolve_failed", cid=cid, error=repr(exc))
            self._later(self._s.unknown_grace * 2, lambda: self._resolve(cid, why))
            return
        before = order.status.value
        if found is not None:
            self._sync_from_view(tracked, found)
            self._reconciled("order_resolved", cid, before, tracked.order.status.value, why)
            self._order_changed(tracked)
            return
        now = self._now()
        if order.status in (S.PENDING_SUBMIT, S.UNKNOWN):
            if self._fresh(order, now) and tracked.attempts < self._s.max_attempts:
                if not self._broker.capabilities.idempotent_submit and not tracked.absent_once:
                    # A resend would not be de-duplicated: make sure the first send is not still in flight.
                    tracked.absent_once = True
                    self._later(self._s.unknown_grace, lambda: self._resolve(cid, why))
                    return
                tracked.absent_once = False
                self._reconciled("order_resubmitted", cid, before, before, f"{why}; broker has no such order")
                tracked.attempts += 1
                await self._persist()
                self._spawn(self._send(cid, triggered_stop_limit=stop_limit is not None))
            else:
                _log.error("oms_order_abandoned", cid=cid, status=before, attempts=tracked.attempts)
                self._reject(tracked, "outcome unknown and not found at the broker; too old to resubmit")
                self._reconciled("order_abandoned", cid, before, S.REJECTED.value, why)
            return
        tracked.order = transition(order, S.CANCELLED, self._ts(order), reason="not found at the broker")
        self._reconciled("order_missing", cid, before, S.CANCELLED.value, why)
        self._order_changed(tracked)

    # ================================================================== broker events

    async def _apply_event(self, event: DomainEvent) -> None:
        if isinstance(event, OrderEvent):
            self._on_order_event(event.order)
        elif isinstance(event, FillEvent):
            self._on_fill(event.fill)
        elif isinstance(event, PositionEvent):
            self._on_position(event.position, event.change)
        elif isinstance(event, AccountEvent):
            self._account = event.state
            self._publish(event)
        elif isinstance(event, ConnectionEvent):
            self._publish(event)
            if event.state is ConnectionState.DOWN:
                self._connected = False
                self.ready.clear()
            elif event.state is ConnectionState.UP:
                self._connected = True
                await self._reconcile(f"connection up: {event.reason or ''}".strip())
        else:
            self._publish(event)

    def _find(self, client_order_id: str | None, broker_order_id: str | None) -> _Tracked | None:
        if client_order_id and client_order_id in self._orders:
            return self._orders[client_order_id]
        if broker_order_id and broker_order_id in self._by_broker_id:
            return self._orders.get(self._by_broker_id[broker_order_id])
        return None

    def _link_broker_id(self, tracked: _Tracked, broker_order_id: str) -> None:
        if tracked.order.broker_order_id != broker_order_id:
            tracked.order = tracked.order.with_changes(broker_order_id=broker_order_id)
        self._by_broker_id[broker_order_id] = tracked.order.client_order_id

    def _on_order_event(self, view: Order) -> None:
        tracked = self._find(view.client_order_id, view.broker_order_id)
        if tracked is None:
            if view.status in _OPEN:
                self._adopt_order(view, "order placed outside the OMS")
            return
        self._sync_from_view(tracked, view)
        self._order_changed(tracked)

    def _adopt_order(self, view: Order, reason: str) -> None:
        tracked = _Tracked(order=view)
        self._orders[view.client_order_id] = tracked
        if view.broker_order_id:
            self._by_broker_id[view.broker_order_id] = view.client_order_id
        self._reconciled("order_adopted", view.client_order_id, None, view.status.value, reason)
        self._order_changed(tracked)

    def _sync_from_view(self, tracked: _Tracked, view: Order) -> None:
        """Bring the local order to the broker's view (the broker wins), never backwards."""
        if view.broker_order_id:
            self._link_broker_id(tracked, view.broker_order_id)
        local = tracked.order
        if not local.status.is_terminal:
            vr, lr = view.request, local.request
            same_type = vr.order_type is lr.order_type  # an emulated STOP_LIMIT goes out as LIMIT/MARKET
            native = self._broker.capabilities.native_sl_tp  # otherwise SL/TP were stripped and are ours
            synced = replace(
                lr,
                limit_price=vr.limit_price if same_type else lr.limit_price,
                stop_price=vr.stop_price if same_type else lr.stop_price,
                sl=vr.sl if native else lr.sl,
                tp=vr.tp if native else lr.tp,
                volume=max(vr.volume, local.filled_volume),
            )
            if synced != lr:
                tracked.order = replace(local, request=synced)
        known = tracked.order.filled_volume
        if view.filled_volume > known:
            price = view.avg_fill_price or view.request.limit_price or view.request.stop_price
            if price is not None:
                delta = view.filled_volume - known
                self._fill(tracked, delta, price, self._ts(tracked.order))
                tracked.synthetic += delta
        if view.status not in _FILL_STATES:
            self._advance(tracked, view.status, view.history[-1].reason or "broker view")

    def _advance(self, tracked: _Tracked, target: OrderStatus, reason: str) -> bool:
        order = tracked.order
        cur = order.status
        if cur is target or cur.is_terminal or target in _FILL_STATES:
            return False
        if not target.is_terminal and cur is not S.UNKNOWN and _RANK.get(target, 9) < _RANK.get(cur, 9):
            return False  # stale
        path: list[OrderStatus]
        if can_transition(cur, target):
            path = [target]
        elif cur in (S.NEW, S.PENDING_SUBMIT) and can_transition(S.ACCEPTED, target):
            path = ([S.PENDING_SUBMIT] if cur is S.NEW else []) + [S.ACCEPTED, target]
        else:
            _log.warning(
                "oms_illegal_transition", cid=order.client_order_id, current=cur.value, target=target.value
            )
            return False
        ts = self._ts(order)
        for status in path:
            tracked.order = transition(tracked.order, status, ts, reason=reason if status is target else None)
        return True

    def _ts(self, order: Order) -> datetime:
        now = self._now()
        return now if now >= order.updated_ts else order.updated_ts

    def _fill(self, tracked: _Tracked, volume: Decimal, price: Decimal, ts: datetime) -> Decimal:
        order = tracked.order
        if order.status in (S.NEW, S.PENDING_SUBMIT):
            self._advance(tracked, S.ACCEPTED, "fill before ACK")
            order = tracked.order
        volume = min(volume, order.remaining_volume)
        if volume <= 0 or not can_transition(
            order.status, S.FILLED if volume == order.remaining_volume else S.PARTIALLY_FILLED
        ):
            return ZERO
        tracked.order = apply_fill(order, volume, price, max(ensure_utc(ts), order.updated_ts))
        return volume

    def _on_fill(self, fill: Fill) -> None:
        if fill.fill_id in self._fill_ids:
            return
        self._fill_ids[fill.fill_id] = None
        tracked = self._find(fill.client_order_id, fill.broker_order_id)
        if tracked is None:
            self._publish(FillEvent(account_id=self._s.account_id, fill=fill, ts=self._now()))
            self._dirty = True
            return
        if fill.broker_order_id:
            self._link_broker_id(tracked, fill.broker_order_id)
        volume = fill.volume
        if tracked.synthetic > 0:  # already applied from a broker order view
            covered = min(volume, tracked.synthetic)
            tracked.synthetic -= covered
            volume -= covered
        if volume > 0:
            self._fill(tracked, volume, fill.price, fill.ts)
        if fill.position_id and fill.side is tracked.order.request.side:
            tracked.position_id = fill.position_id
            self._pos_strategy.setdefault(fill.position_id, tracked.order.request.strategy_id)
        self._publish(
            FillEvent(
                account_id=self._s.account_id,
                fill=replace(fill, client_order_id=tracked.order.client_order_id),
                ts=self._now(),
            )
        )
        self._order_changed(tracked)

    def _attribute(self, pos: Position) -> Position:
        strategy = self._pos_strategy.get(pos.position_id)
        if strategy is not None and strategy != pos.strategy_id:
            return replace(pos, strategy_id=strategy)
        if pos.strategy_id != MANUAL and pos.position_id not in self._pos_strategy:
            self._pos_strategy[pos.position_id] = pos.strategy_id
        return pos

    def _on_position(self, pos: Position, change: PositionChange) -> None:
        pos = self._attribute(pos)
        if change is PositionChange.CLOSED:
            self._positions.pop(pos.position_id, None)
            self.emulation.drop_position(pos.position_id)
            self._pos_strategy.pop(pos.position_id, None)
        else:
            self._positions[pos.position_id] = pos
            self.emulation.note_stop(pos.position_id, pos.sl)
            self._arm_waiting_exits(pos.position_id)
        self._dirty = True
        self._publish(
            PositionEvent(account_id=self._s.account_id, position=pos, change=change, ts=self._now())
        )

    # ================================================================== order follow-ups

    def _order_changed(self, tracked: _Tracked) -> None:
        order = tracked.order
        self._dirty = True
        if (
            tracked.waiter is not None
            and not tracked.waiter.done()
            and order.status not in (S.NEW, S.PENDING_SUBMIT)
        ):
            tracked.waiter.set_result(order)
        if order.filled_volume > 0 and order.request.oco_group and not tracked.oco_done:
            tracked.oco_done = True
            for other in list(self._orders.values()):
                r = other.order.request
                if (
                    other is not tracked
                    and r.oco_group == order.request.oco_group
                    and r.strategy_id == order.request.strategy_id
                    and other.order.status in _OPEN
                ):
                    self._spawn_cancel(other, f"OCO: {order.client_order_id} filled")
        if order.status is S.FILLED:
            self.emulation.drop_order(order.client_order_id)
            self._arm_after_fill(tracked)
        elif order.status.is_terminal:
            self.emulation.drop_order(order.client_order_id)
        self._publish(
            OrderEvent(account_id=self._s.account_id, order=order, previous_status=None, ts=self._now())
        )

    def _arm_waiting_exits(self, position_id: str) -> None:
        for tracked in self._orders.values():
            if tracked.position_id == position_id and tracked.order.status is S.FILLED:
                self._arm_after_fill(tracked)

    def _arm_after_fill(self, tracked: _Tracked) -> None:
        order, pid = tracked.order, tracked.position_id
        if pid is None or order.avg_fill_price is None:
            return  # wait for the fill that tells us the position
        req = order.request
        if req.exits and not tracked.exits_armed:
            tracked.exits_armed = True
            self.emulation.arm_exits(
                position_id=pid,
                symbol=req.symbol,
                side=req.side,
                entry=order.avg_fill_price,
                volume=order.filled_volume,
                opened=order.updated_ts,
                exits=req.exits,
            )
        if (
            (req.sl is not None or req.tp is not None)
            and not self._broker.capabilities.native_sl_tp
            and not tracked.sltp_attached
        ):
            tracked.sltp_attached = True
            self.emulation.arm_sl_tp(position_id=pid, symbol=req.symbol, side=req.side, sl=req.sl, tp=req.tp)
        self._dirty = True

    def _spawn_cancel(self, tracked: _Tracked, reason: str) -> None:
        order = tracked.order
        stop_limit = self.emulation.stop_limit(order.client_order_id)
        if stop_limit is not None and not stop_limit.triggered:
            tracked.order = transition(order, S.CANCELLED, self._ts(order), reason=reason)
            self._order_changed(tracked)
            return
        if order.broker_order_id is None:
            _log.warning("oms_cancel_without_broker_id", cid=order.client_order_id, reason=reason)
            return
        broker_id = order.broker_order_id

        async def go() -> None:
            try:
                await self._broker.cancel_order(broker_id)
            except BrokerError as exc:
                _log.warning("oms_cancel_failed", cid=order.client_order_id, error=repr(exc))

        self._spawn(go())

    # ================================================================== manual / strategy commands

    async def _do_modify(self, cid: str, changes: OrderChanges) -> BrokerAck | None:
        tracked = self._orders.get(cid)
        if tracked is None or tracked.order.status not in _OPEN:
            raise OrderNotFound(cid)
        stop_limit = self.emulation.stop_limit(cid)
        if stop_limit is not None and not stop_limit.triggered:
            r = tracked.order.request
            tracked.order = replace(
                tracked.order,
                request=replace(
                    r,
                    stop_price=changes.stop_price or r.stop_price,
                    limit_price=changes.limit_price or r.limit_price,
                    volume=changes.volume or r.volume,
                    sl=None if changes.remove_sl else changes.sl or r.sl,
                    tp=None if changes.remove_tp else changes.tp or r.tp,
                ),
            )
            if changes.stop_price is not None:
                stop_limit.stop = changes.stop_price
            self._order_changed(tracked)
            return None
        if tracked.order.broker_order_id is None:
            raise OrderRejected(f"{cid} has no broker order id yet")
        return await self._broker.modify_order(tracked.order.broker_order_id, changes)

    async def _do_cancel(self, cid: str, reason: str) -> BrokerAck | None:
        tracked = self._orders.get(cid)
        if tracked is None or tracked.order.status not in _OPEN:
            raise OrderNotFound(cid)
        stop_limit = self.emulation.stop_limit(cid)
        if stop_limit is not None and not stop_limit.triggered:
            self._spawn_cancel(tracked, reason)
            return None
        if tracked.order.broker_order_id is None:
            raise OrderRejected(f"{cid} has no broker order id yet")
        return await self._broker.cancel_order(tracked.order.broker_order_id)

    async def _do_modify_position(self, pid: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck | None:
        pos = self._positions.get(pid)
        if pos is None:
            raise PositionNotFound(pid)
        if not self._broker.capabilities.native_sl_tp:
            self.emulation.arm_sl_tp(position_id=pid, symbol=pos.symbol, side=pos.side, sl=sl, tp=tp)
            self._dirty = True
            return None
        ack = await self._broker.modify_position(pid, sl, tp)
        self.emulation.note_stop(pid, sl)
        return ack

    async def _do_close(self, pid: str, volume: Decimal | None) -> BrokerAck:
        if pid not in self._positions:
            raise PositionNotFound(pid)
        return await self._broker.close_position(pid, volume)

    async def _do_close_all(self, symbol: str | None, strategy_id: str | None, cancel_pending: bool) -> int:
        sent = 0
        if cancel_pending:
            for tracked in list(self._orders.values()):
                r = tracked.order.request
                if (
                    tracked.order.status in (S.ACCEPTED, S.WORKING, S.PARTIALLY_FILLED)
                    and (symbol is None or r.symbol == symbol)
                    and (strategy_id is None or r.strategy_id == strategy_id)
                ):
                    self._spawn_cancel(tracked, "close all")
                    sent += 1
        for pos in list(self._positions.values()):
            if (symbol is None or pos.symbol == symbol) and (
                strategy_id is None or pos.strategy_id == strategy_id
            ):
                self._spawn_close(pos.position_id, None, "close all")
                sent += 1
        return sent

    def _spawn_close(self, pid: str, volume: Decimal | None, reason: str) -> None:
        async def go() -> None:
            try:
                await self._broker.close_position(pid, volume)
            except BrokerError as exc:
                _log.warning("oms_close_failed", position_id=pid, reason=reason, error=repr(exc))

        self._spawn(go())

    # ================================================================== emulation

    async def _emulate_tick(self, tick: Tick) -> None:
        actions = self.emulation.on_tick(tick, self._positions, self._instruments)
        await self._execute(actions)

    async def _execute(self, actions: Sequence[Action]) -> None:
        for action in actions:
            self._dirty = True
            _log.info("oms_emulated_action", action=type(action).__name__, reason=action.reason)
            if isinstance(action, ModifyStop):
                pos = self._positions.get(action.position_id)
                if pos is None:
                    continue
                if self._broker.capabilities.native_sl_tp:
                    pid, sl, tp = action.position_id, action.sl, pos.tp

                    async def modify(pid: str = pid, sl: Decimal = sl, tp: Decimal | None = tp) -> None:
                        try:
                            await self._broker.modify_position(pid, sl, tp)
                        except BrokerError as exc:
                            _log.warning("oms_emulated_modify_failed", position_id=pid, error=repr(exc))

                    self._spawn(modify())
                else:
                    self.emulation.arm_sl_tp(
                        position_id=pos.position_id, symbol=pos.symbol, side=pos.side, sl=action.sl, tp=pos.tp
                    )
            elif isinstance(action, ClosePosition):
                if action.position_id in self._positions:
                    self._spawn_close(action.position_id, action.volume, action.reason)
            elif isinstance(action, CancelOrder):
                tracked = self._orders.get(action.client_order_id)
                if tracked is not None and tracked.order.status in _OPEN:
                    self._spawn_cancel(tracked, action.reason)
            elif isinstance(action, SendStopLimit):
                tracked = self._orders.get(action.client_order_id)
                if tracked is not None and tracked.order.status in _OPEN:
                    tracked.first_sent = self._now()
                    tracked.attempts += 1
                    await self._persist()  # the trigger is durable before the limit leg leaves
                    self._spawn(self._send(action.client_order_id, triggered_stop_limit=True))

    # ================================================================== reconcile

    def _reconciled(self, kind: str, entity: str, before: str | None, after: str | None, reason: str) -> None:
        _log.info("oms_reconcile", kind=kind, entity=entity, before=before, after=after, reason=reason)
        self._publish(
            ReconcileEvent(
                account_id=self._s.account_id,
                kind=kind,
                entity_id=entity,
                before=before,
                after=after,
                reason=reason,
                ts=self._now(),
            )
        )
        self._dirty = True

    async def _reconcile(self, reason: str) -> bool:
        """Snapshot the broker and make local state match it (the broker wins). Sets ``ready`` on success."""
        try:
            snap = await self._broker.snapshot()
        except BrokerError as exc:
            _log.warning("oms_reconcile_failed", reason=reason, error=repr(exc))
            if self._connected:
                self._later(timedelta(seconds=5), lambda: self._reconcile_job(reason))
            return False
        self._account = snap.account
        broker_pos = {p.position_id: self._attribute(p) for p in snap.positions}
        for pid, pos in broker_pos.items():
            local = self._positions.get(pid)
            if local is None:
                kind = "position_adopted" if pos.strategy_id == MANUAL else "position_restored"
                self._reconciled(kind, pid, None, _pos_str(pos), reason)
            elif (local.volume, local.side, local.sl, local.tp, local.strategy_id) != (
                pos.volume,
                pos.side,
                pos.sl,
                pos.tp,
                pos.strategy_id,
            ):
                self._reconciled("position_corrected", pid, _pos_str(local), _pos_str(pos), reason)
            self._positions[pid] = pos
            self.emulation.note_stop(pid, pos.sl)
        for pid in [p for p in self._positions if p not in broker_pos]:
            self._reconciled("position_missing", pid, _pos_str(self._positions[pid]), None, reason)
            del self._positions[pid]
            self.emulation.drop_position(pid)
        seen: set[str] = set()
        for view in snap.orders:
            tracked = self._find(view.client_order_id, view.broker_order_id)
            if tracked is None:
                self._adopt_order(view, reason)
                seen.add(view.client_order_id)
                continue
            before = (
                tracked.order.status,
                tracked.order.request.limit_price,
                tracked.order.request.stop_price,
            )
            self._sync_from_view(tracked, view)
            after = (
                tracked.order.status,
                tracked.order.request.limit_price,
                tracked.order.request.stop_price,
            )
            if before != after:
                self._reconciled(
                    "order_corrected", tracked.order.client_order_id, str(before[0]), str(after[0]), reason
                )
            self._order_changed(tracked)
            seen.add(tracked.order.client_order_id)
        for cid in [c for c, t in self._orders.items() if t.order.status in _OPEN and c not in seen]:
            await self._resolve(cid, f"reconcile ({reason}): not among the broker's open orders")
        for pid in list(self._positions):
            self._arm_waiting_exits(pid)
        self._last_reconcile = self._now()
        self._dirty = True
        if self._connected:
            self.ready.set()
        if snap.account is not None:
            self._publish(AccountEvent(account_id=self._s.account_id, state=snap.account, ts=self._now()))
        return True

    async def _reconcile_job(self, reason: str) -> None:
        await self._reconcile(reason)


def _pos_str(p: Position) -> str:
    return f"{p.side.value} {p.volume} {p.symbol} @ {p.entry_price} sl={p.sl} tp={p.tp} [{p.strategy_id}]"
