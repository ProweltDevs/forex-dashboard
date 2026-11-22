"""Prometheus metrics (SPEC §15): latencies, tick rates, queue depths, order counts, rejects, reconnects,
risk events, plus equity, kill level, strategy state, database and notification health.

- ``Metrics``: every metric, in its own ``CollectorRegistry`` (no global state: tests and several apps in
  one process stay independent).
- ``OpsMonitor``: feeds them. It consumes the app's event buses through bounded ``DROP_OLDEST``
  subscriptions (metrics must never hold memory or the loop hostage), samples gauges every
  ``ops.sample_s``, measures event-loop lag, and watches the **clock offset** between this host and each
  broker feed (receive time minus the broker's tick time; the minimum over a minute approximates the
  offset plus the smallest network latency). A drift beyond ``ops.clock_drift_alert_s`` publishes an
  ``AlertEvent("clock_drift")`` on ``events``, which notifications, persistence and the stream pick up.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import OrderedDict, deque
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Info

from trader.domain.enums import ConnectionState, FeedState, OrderStatus, OrderType
from trader.domain.events import (
    AccountEvent,
    AlertEvent,
    ConnectionEvent,
    DomainEvent,
    FeedEvent,
    FillEvent,
    OrderEvent,
    OrderRetryEvent,
    ReconcileEvent,
    RiskEvent,
    RiskStateEvent,
    StrategyEvent,
    TickEvent,
)
from trader.domain.models import Order
from trader.domain.time import Clock
from trader.marketdata.bus import Bus, Overflow, Subscription

from .logging import get_logger

if TYPE_CHECKING:
    from trader.app import App

_log = get_logger("trader.metrics")

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
OFFSET_WINDOW = timedelta(seconds=60)
_SEEN_LIMIT = 20_000


def reject_rule(reason: str | None) -> str:
    """A stable, low-cardinality name for a rejection: ``risk:<rule>`` or the broker error's first part."""
    text = reason or "rejected"
    if text.startswith("risk:"):
        return ":".join(text.split(":")[:2])
    return text.split(":")[0][:60]


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r = self.registry = registry or CollectorRegistry()
        self.build = Info("trader_build", "Version and mode of the running process", registry=r)
        self.ticks = Counter("trader_ticks", "Ticks received", ["feed", "symbol"], registry=r)
        self.tick_age = Gauge(
            "trader_last_tick_age_seconds", "Seconds since the last tick", ["feed", "symbol"], registry=r
        )
        self.feed_stale = Gauge(
            "trader_feed_stale", "1 while a feed is stale", ["feed", "symbol"], registry=r
        )
        self.clock_offset = Gauge(
            "trader_clock_offset_seconds",
            "Receive time minus broker tick time, minimum over the last minute",
            ["feed"],
            registry=r,
        )
        self.orders = Counter(
            "trader_order_status_changes",
            "Order status changes",
            ["account", "strategy", "status"],
            registry=r,
        )
        self.rejects = Counter(
            "trader_order_rejects", "Rejected orders", ["account", "strategy", "rule"], registry=r
        )
        self.retries = Counter("trader_order_retries", "Order resends", ["account", "error"], registry=r)
        self.order_latency = Histogram(
            "trader_order_latency_seconds",
            "send_to_ack: PENDING_SUBMIT to ACCEPTED; ack_to_fill, submit_to_fill: market orders only",
            ["account", "leg"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.fills = Counter("trader_fills", "Fills", ["account", "symbol"], registry=r)
        self.fill_lots = Counter(
            "trader_fill_volume_lots", "Filled volume", ["account", "symbol"], registry=r
        )
        self.connected = Gauge("trader_broker_connected", "1 while connected", ["account"], registry=r)
        self.ready = Gauge("trader_account_ready", "1 while reconciled and trading", ["account"], registry=r)
        self.disconnects = Counter("trader_broker_disconnects", "Connection losses", ["account"], registry=r)
        self.reconnects = Counter("trader_broker_reconnects", "Reconnections", ["account"], registry=r)
        self.reconciles = Counter(
            "trader_reconcile_corrections", "Reconcile corrections", ["account", "kind"], registry=r
        )
        self.risk = Counter(
            "trader_risk_decisions", "Pre-trade risk decisions", ["account", "decision", "rule"], registry=r
        )
        self.kill_level = Gauge(
            "trader_kill_level",
            "0 NONE, 1 PAUSE_NEW, 2 CANCEL_PENDING, 3 FLATTEN_ALL",
            ["account"],
            registry=r,
        )
        self.kill_changes = Counter(
            "trader_kill_level_changes", "Kill switch / breaker changes", ["account", "level"], registry=r
        )
        self.alerts = Counter("trader_alerts", "Operator alerts", ["kind", "severity"], registry=r)
        self.positions = Gauge("trader_open_positions", "Open positions", ["account"], registry=r)
        self.open_orders = Gauge("trader_open_orders", "Open orders", ["account"], registry=r)
        self.equity = Gauge("trader_equity", "Equity", ["account", "source"], registry=r)
        self.drawdown = Gauge("trader_drawdown", "High-water mark minus equity", ["account"], registry=r)
        self.strategy_status = Gauge(
            "trader_strategy_status",
            "1 for the strategy's current status",
            ["strategy", "status"],
            registry=r,
        )
        self.strategy_events = Counter(
            "trader_strategy_events", "Strategy lifecycle events", ["strategy", "kind"], registry=r
        )
        self.bus_depth = Gauge(
            "trader_bus_queue_depth", "Items waiting in event-bus subscriptions", ["subscriber"], registry=r
        )
        self.bus_dropped = Gauge(
            "trader_bus_dropped", "Items dropped or superseded (cumulative)", ["subscriber"], registry=r
        )
        self.db_up = Gauge("trader_db_up", "1 while the database accepts writes", registry=r)
        self.db_pending = Gauge("trader_db_pending_records", "Records not yet in the database", registry=r)
        self.notify_sent = Gauge("trader_notifications_sent", "Notifications sent (cumulative)", registry=r)
        self.notify_failed = Gauge("trader_notifications_failed", "Notifications given up", registry=r)
        self.loop_lag = Gauge(
            "trader_event_loop_lag_seconds", "Worst event-loop delay in the last sample", registry=r
        )
        self.started = Gauge("trader_start_time_seconds", "Process start (unix time)", registry=r)


_STATUS_ORDER = (OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED, OrderStatus.FILLED)


def _first(order: Order, status: OrderStatus) -> datetime | None:
    return next((c.ts for c in order.history if c.status is status), None)


class OpsMonitor:
    def __init__(
        self,
        app: App,
        metrics: Metrics,
        clock: Clock,
        *,
        sample: timedelta = timedelta(seconds=5),
        drift_alert: timedelta = timedelta(seconds=2),
    ) -> None:
        self._app = app
        self.metrics = metrics
        self._clock = clock
        self._sample = sample
        self._drift = drift_alert
        self.events: Bus[DomainEvent] = Bus()  # ops alerts (clock drift)
        self._subs: list[Subscription[Any]] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._status: OrderedDict[str, OrderStatus] = OrderedDict()
        self._offsets: dict[str, deque[tuple[datetime, float]]] = {}
        self._drifting: set[str] = set()
        self._connection: dict[str, ConnectionState] = {}
        self._lag = 0.0
        self.stale: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ lifecycle

    def attach(self, bus: Bus[Any], name: str, *, maxsize: int = 20_000) -> None:
        sub = bus.subscribe(f"metrics-{name}", policy=Overflow.DROP_OLDEST, maxsize=maxsize)
        self._subs.append(sub)
        self._tasks.append(asyncio.create_task(self._consume(sub), name=f"metrics-{name}"))

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._sampler(), name="metrics-sampler"))
        self._tasks.append(asyncio.create_task(self._lag_probe(), name="metrics-loop-lag"))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        self.events.close()

    async def _consume(self, sub: Subscription[Any]) -> None:
        async for event in sub:
            try:
                self.observe(event)
            except Exception:
                _log.exception("metrics_event_failed", kind=type(event).__name__)

    # ------------------------------------------------------------------ events

    def observe(self, event: object) -> None:
        m = self.metrics
        match event:
            case TickEvent():
                m.ticks.labels(event.broker, event.tick.symbol).inc()
                if event.tick.broker_ts is not None:
                    offset = (event.tick.recv_ts - event.tick.broker_ts).total_seconds()
                    window = self._offsets.setdefault(event.broker, deque())
                    window.append((event.tick.recv_ts, offset))
            case OrderEvent():
                self._order(event)
            case FillEvent():
                m.fills.labels(event.account_id, event.fill.symbol).inc()
                m.fill_lots.labels(event.account_id, event.fill.symbol).inc(float(event.fill.volume))
            case OrderRetryEvent():
                m.retries.labels(event.account_id, event.error).inc()
            case ConnectionEvent():
                before = self._connection.get(event.account_id)
                self._connection[event.account_id] = event.state
                m.connected.labels(event.account_id).set(1 if event.state is ConnectionState.UP else 0)
                if event.state is ConnectionState.DOWN and before is not ConnectionState.DOWN:
                    m.disconnects.labels(event.account_id).inc()
                elif event.state is ConnectionState.UP and before not in (None, ConnectionState.UP):
                    m.reconnects.labels(event.account_id).inc()
            case FeedEvent():
                stale = event.state is FeedState.STALE
                m.feed_stale.labels(event.broker, event.symbol).set(1 if stale else 0)
                (self.stale.add if stale else self.stale.discard)((event.broker, event.symbol))
            case ReconcileEvent():
                m.reconciles.labels(event.account_id, event.kind).inc()
            case RiskEvent():
                m.risk.labels(event.account_id, str(event.decision), event.rule).inc()
            case RiskStateEvent():
                m.kill_level.labels(event.account_id).set(event.level.rank)
                m.kill_changes.labels(event.account_id, str(event.level)).inc()
            case AlertEvent():
                m.alerts.labels(event.kind, event.severity).inc()
            case StrategyEvent():
                m.strategy_events.labels(event.strategy_id, event.kind).inc()
            case AccountEvent():
                m.equity.labels(event.account_id, "broker").set(float(event.state.equity))

    def _order(self, event: OrderEvent) -> None:
        o, m = event.order, self.metrics
        cid, r = o.request.client_order_id, o.request
        before = self._status.get(cid)
        if before is o.status:
            return
        self._status[cid] = o.status
        self._status.move_to_end(cid)
        if len(self._status) > _SEEN_LIMIT:
            self._status.popitem(last=False)
        m.orders.labels(event.account_id, r.strategy_id, str(o.status)).inc()
        if o.status is OrderStatus.REJECTED:
            m.rejects.labels(event.account_id, r.strategy_id, reject_rule(o.rejection_reason)).inc()
        sent, acked, filled = (_first(o, s) for s in _STATUS_ORDER)
        if o.status is OrderStatus.ACCEPTED and sent and acked:
            m.order_latency.labels(event.account_id, "send_to_ack").observe((acked - sent).total_seconds())
        if o.status is OrderStatus.FILLED and r.order_type is OrderType.MARKET and filled:
            if acked:
                m.order_latency.labels(event.account_id, "ack_to_fill").observe(
                    (filled - acked).total_seconds()
                )
            if sent:
                m.order_latency.labels(event.account_id, "submit_to_fill").observe(
                    (filled - sent).total_seconds()
                )

    # ------------------------------------------------------------------ sampling

    async def _lag_probe(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            await asyncio.sleep(0.25)
            self._lag = max(self._lag, loop.time() - start - 0.25)

    async def _sampler(self) -> None:
        while True:
            try:
                self.sample()
            except Exception:
                _log.exception("metrics_sample_failed")
            await asyncio.sleep(self._sample.total_seconds())

    def sample(self) -> None:
        app, m, now = self._app, self.metrics, self._clock.now()
        m.loop_lag.set(self._lag)
        self._lag = 0.0
        for aid, s in app.accounts.items():
            view = s.oms.view()
            m.connected.labels(aid).set(1 if view.broker_connected else 0)
            m.ready.labels(aid).set(1 if s.oms.ready.is_set() else 0)
            m.positions.labels(aid).set(len(view.positions))
            m.open_orders.labels(aid).set(len(view.open_orders))
            m.kill_level.labels(aid).set(s.oms.risk_level.rank)
            if view.account is not None:
                m.equity.labels(aid, "broker").set(float(view.account.equity))
            books = app.books.get(aid)
            point = books.portfolio.snapshot() if books is not None else None
            if point is not None:
                m.equity.labels(aid, "computed").set(float(point.equity))
                m.drawdown.labels(aid).set(float(point.drawdown))
            for symbol in s.account.symbols:
                tick = app.market.last_tick(symbol, aid)
                if tick is not None:
                    m.tick_age.labels(aid, symbol).set(max(0.0, (now - tick.recv_ts).total_seconds()))
        if app.runner is not None:
            for sid, status, _reason, _stats in app.runner.status():
                for st in type(status):
                    m.strategy_status.labels(sid, str(st)).set(1 if st is status else 0)
        depth: dict[str, int] = {}
        dropped: dict[str, int] = {}
        for bus in app.event_buses():
            for sub in bus.subscribers:
                depth[sub.name] = depth.get(sub.name, 0) + len(sub)
                dropped[sub.name] = dropped.get(sub.name, 0) + sub.dropped
        for name, value in depth.items():
            m.bus_depth.labels(name).set(value)
            m.bus_dropped.labels(name).set(dropped[name])
        if app.persistence is not None:
            writer = app.persistence.writer
            m.db_up.set(0 if writer.down else 1)
            m.db_pending.set(writer.pending)
        if app.notifications is not None:
            stats = app.notifications.notifier.stats
            m.notify_sent.set(stats.sent)
            m.notify_failed.set(stats.failed)
        self.check_clock(now)

    def clock_offsets(self) -> dict[str, float]:
        return {feed: min(o for _t, o in w) for feed, w in self._offsets.items() if w}

    def check_clock(self, now: datetime) -> None:
        for feed, window in self._offsets.items():
            while window and now - window[0][0] > OFFSET_WINDOW:
                window.popleft()
            if not window:
                continue
            offset = min(o for _t, o in window)
            self.metrics.clock_offset.labels(feed).set(offset)
            limit = self._drift.total_seconds()
            if abs(offset) > limit and feed not in self._drifting:
                self._drifting.add(feed)
                self._alert(
                    "clock_drift",
                    "warning",
                    f"{feed}: local clock differs from the broker's by {offset:+.2f} s "
                    f"(limit {limit:.1f} s); check NTP/chrony on this host",
                    now,
                )
            elif abs(offset) <= limit / 2 and feed in self._drifting:
                self._drifting.discard(feed)
                self._alert(
                    "clock_drift_cleared", "info", f"{feed}: clock offset back to {offset:+.2f} s", now
                )

    def _alert(self, kind: str, severity: str, message: str, now: datetime) -> None:
        _log.warning(kind, message=message)
        self.events.publish(AlertEvent(account_id="*", kind=kind, severity=severity, message=message, ts=now))
