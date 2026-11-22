"""RiskEngine: pre-trade rules, kill switch and circuit breakers for one account (SPEC §7, ADR 0008).

The kill-switch level is the highest level among the active *triggers*:

| trigger | set by | clears |
|---|---|---|
| ``manual`` | ``kill()`` (API/CLI) | ``reset()`` |
| ``daily_loss`` | equity fell by the daily limit since the day start | next trading day, or ``reset()`` |
| ``drawdown`` | equity fell by ``max_drawdown_pct`` from its peak | ``reset()`` (re-bases the peak) |
| ``rejects`` | N broker rejects in a row | ``reset()`` |
| ``reconcile`` | a configured reconcile correction (e.g. ``position_corrected``) | ``reset()`` |
| ``feed_stale`` | a traded symbol without ticks for T while its market is open | ticks resume |
| ``disconnected`` | broker down for longer than T | reconnect |
| ``clock_drift`` | median receive-vs-broker timestamp offset above the threshold | back in range |

At PAUSE_NEW and above every new intent is rejected; closes and cancels are always allowed (they reduce
risk). At CANCEL_PENDING the OMS cancels every pending order, at FLATTEN_ALL it also closes every position,
immediately on escalation and again every ``ENFORCE_EVERY`` while anything is left.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal

from trader.config.risk import RiskConfig
from trader.domain.enums import ConnectionState, KillLevel, OrderStatus, Side
from trader.domain.events import (
    AccountEvent,
    ConnectionEvent,
    DomainEvent,
    FillEvent,
    OrderEvent,
    ReconcileEvent,
    RiskStateEvent,
)
from trader.domain.jsontypes import JsonObj, JsonValue
from trader.domain.models import AccountState, OrderIntent, Tick
from trader.domain.time import Clock, from_epoch_ms, to_epoch_ms
from trader.observability.logging import get_logger

from .gate import NOTHING, AccountView, RiskGate, RiskOutput, RiskVerdict
from .market import ClockDrift, FeedClock, SpreadTracker
from .rules import RULES, FlowStats, RuleInput, daily_loss_breach, drawdown_breach, in_weekend

_log = get_logger("trader.risk")

type Clears = Literal["manual", "condition", "new_day"]

ENFORCE_EVERY = timedelta(seconds=5)
MINUTE = timedelta(minutes=1)


@dataclass(slots=True)
class Trigger:
    level: KillLevel
    reason: str
    since: datetime
    clears: Clears


class RiskEngine(RiskGate):
    def __init__(self, config: RiskConfig, clock: Clock, *, account_id: str) -> None:
        self._cfg = config
        self._clock = clock
        self._account_id = account_id
        self.spreads = SpreadTracker()
        self.feed = FeedClock()
        self.drift = ClockDrift()
        self._orders: deque[tuple[datetime, str]] = deque()  # approved intents: (ts, strategy_id)
        self._fills: deque[datetime] = deque()
        self._signals: dict[tuple[str, str, Side], datetime] = {}
        self._triggers: dict[str, Trigger] = {}
        self._day: date | None = None
        self._day_start: Decimal | None = None
        self._peak: Decimal | None = None
        self._rejects = 0
        self._started = clock.now()
        self._disconnected_since: datetime | None = None
        self._last_enforce: datetime | None = None

    # ================================================================== state

    @property
    def level(self) -> KillLevel:
        return max((t.level for t in self._triggers.values()), key=lambda lv: lv.rank, default=KillLevel.NONE)

    @property
    def triggers(self) -> dict[str, Trigger]:
        return dict(self._triggers)

    @property
    def day_start_equity(self) -> Decimal | None:
        return self._day_start

    @property
    def peak_equity(self) -> Decimal | None:
        return self._peak

    def trading_day(self, ts: datetime) -> date:
        return (ts - timedelta(minutes=self._cfg.day_offset_minutes)).date()

    def _now(self) -> datetime:
        return self._clock.now()

    # ================================================================== pre-trade

    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        now = self._now()
        self._note_equity(view.account, now)
        if self.level is not KillLevel.NONE:
            return RiskVerdict.reject("kill_switch", f"{self.level}: {self._summary()}")
        limits = self._cfg.for_strategy(intent.strategy_id)
        inp = RuleInput(
            intent=intent,
            view=view,
            limits=limits,
            now=now,
            flow=self._flow(intent, now, limits.order_to_fill_window_min),
            typical_spread=self.spreads.typical(intent.symbol),
            day_start_equity=self._day_start,
            peak_equity=self._peak,
        )
        for name, rule in RULES.items():
            why = rule(inp)
            if why is not None:
                return RiskVerdict.reject(name, why)
        self._orders.append((now, intent.strategy_id))
        self._signals[(intent.strategy_id, intent.symbol, intent.side)] = now
        return RiskVerdict.approve(rule="all_rules", reason=f"{len(RULES)} rules passed")

    def _flow(self, intent: OrderIntent, now: datetime, window_min: int) -> FlowStats:
        horizon = now - max(MINUTE, timedelta(minutes=window_min))
        while self._orders and self._orders[0][0] < horizon:
            self._orders.popleft()
        while self._fills and self._fills[0] < horizon:
            self._fills.popleft()
        minute = now - MINUTE
        window = now - timedelta(minutes=window_min)
        recent = [(ts, sid) for ts, sid in self._orders if ts >= minute]
        return FlowStats(
            orders_last_minute=len(recent),
            strategy_orders_last_minute=sum(1 for _, sid in recent if sid == intent.strategy_id),
            window_orders=sum(1 for ts, _ in self._orders if ts >= window),
            window_fills=sum(1 for ts in self._fills if ts >= window),
            last_same_signal=self._signals.get((intent.strategy_id, intent.symbol, intent.side)),
        )

    # ================================================================== feeds

    def on_tick(self, tick: Tick) -> None:
        self.spreads.on_tick(tick)
        self.feed.on_tick(tick)
        self.drift.on_tick(tick)

    def observe(self, event: DomainEvent, view: AccountView) -> RiskOutput:
        now = self._now()
        if isinstance(event, AccountEvent):
            changed = self._note_equity(event.state, now)
            return self._check_losses(view, now) | RiskOutput(changed=changed)
        if isinstance(event, FillEvent):
            self._fills.append(now)
            self._rejects = 0
        elif isinstance(event, OrderEvent):
            return self._on_order(event, view, now)
        elif isinstance(event, ReconcileEvent) and event.kind in self._cfg.breakers.reconcile_kinds:
            why = f"reconcile correction {event.kind} {event.entity_id}: {event.before} -> {event.after}"
            return self._set("reconcile", self._cfg.breakers.reconcile_level, why, "manual", view, now)
        elif isinstance(event, ConnectionEvent):
            if event.state is ConnectionState.DOWN and self._disconnected_since is None:
                self._disconnected_since = now
            elif event.state is ConnectionState.UP:
                self._disconnected_since = None
        return NOTHING

    def _on_order(self, event: OrderEvent, view: AccountView, now: datetime) -> RiskOutput:
        order = event.order
        if order.status in (OrderStatus.ACCEPTED, OrderStatus.WORKING):
            self._rejects = 0
        elif order.status is OrderStatus.REJECTED and any(
            h.status is OrderStatus.PENDING_SUBMIT for h in order.history
        ):
            self._rejects += 1  # rejected after it was sent (risk rejects never reach PENDING_SUBMIT)
            cap = self._cfg.breakers.max_consecutive_rejects
            if cap is not None and self._rejects >= cap:
                why = f"{self._rejects} broker rejects in a row (last: {order.rejection_reason})"
                return self._set("rejects", self._cfg.breakers.rejects_level, why, "manual", view, now)
        return NOTHING

    def on_time(self, view: AccountView) -> RiskOutput:
        now = self._now()
        out = RiskOutput(changed=self._note_equity(view.account, now))
        if self._day is not None and self.trading_day(now) != self._day:
            out |= self._new_day(view, now)
        out |= self._check_losses(view, now)
        out |= self._check_feed(view, now)
        out |= self._check_connection(view, now)
        out |= self._check_drift(view, now)
        return out | self._enforce(view, now, escalated=False)

    # ================================================================== manual

    def kill(self, level: KillLevel, reason: str, view: AccountView) -> RiskOutput:
        if level is KillLevel.NONE:
            raise ValueError("use reset() to clear the kill switch")
        _log.warning("kill_switch", account=self._account_id, level=str(level), reason=reason)
        self._triggers.pop("manual", None)  # a manual switch may also be lowered
        return self._set("manual", level, reason, "manual", view, self._now(), force_event=True)

    def reset(self, reason: str, view: AccountView) -> RiskOutput:
        now = self._now()
        previous = self.level
        cleared = sorted(self._triggers)
        self._triggers.clear()
        self._rejects = 0
        if view.account is not None and "drawdown" in cleared:
            self._peak = view.account.equity  # acknowledging a drawdown re-bases the peak
        _log.warning("kill_switch_reset", account=self._account_id, reason=reason, cleared=cleared)
        event = self._state_event(
            previous, f"reset: {reason} (cleared {', '.join(cleared) or 'nothing'})", now
        )
        return RiskOutput(events=(event,), changed=True)

    # ================================================================== breakers

    def _check_losses(self, view: AccountView, now: datetime) -> RiskOutput:
        if view.account is None:
            return NOTHING
        limits, b = self._cfg.limits, self._cfg.breakers
        out = NOTHING
        if self._day_start is not None and (why := daily_loss_breach(limits, view, self._day_start)):
            out |= self._set("daily_loss", b.daily_loss_level, why, "new_day", view, now)
        if self._peak is not None and (why := drawdown_breach(limits, view, self._peak)):
            out |= self._set("drawdown", b.drawdown_level, why, "manual", view, now)
        return out

    def _check_feed(self, view: AccountView, now: datetime) -> RiskOutput:
        limit = self._cfg.breakers.feed_stale_s
        if limit is None or not view.broker_connected:
            return self._clear("feed_stale", view, now)
        weekend = self._cfg.limits.sessions.weekend
        symbols = set(self.feed.last) | {p.symbol for p in view.positions}
        stale = []
        for sym in sorted(symbols):
            inst = view.instruments.get(sym)
            if inst is None or not inst.is_open(now) or (weekend is not None and in_weekend(now, weekend)):
                continue  # no ticks expected
            last = max(self.feed.last.get(sym, self._started), self._started)
            if now - last > timedelta(seconds=float(limit)):
                stale.append(f"{sym} {(now - last).total_seconds():.0f}s")
        if not stale:
            return self._clear("feed_stale", view, now)
        why = f"no ticks: {', '.join(stale)} (max {limit}s)"
        return self._set("feed_stale", self._cfg.breakers.feed_stale_level, why, "condition", view, now)

    def _check_connection(self, view: AccountView, now: datetime) -> RiskOutput:
        limit = self._cfg.breakers.disconnected_s
        if view.broker_connected:
            self._disconnected_since = None
            return self._clear("disconnected", view, now)
        since = self._disconnected_since = self._disconnected_since or now
        if limit is None or now - since <= timedelta(seconds=float(limit)):
            return NOTHING
        why = f"broker disconnected for {(now - since).total_seconds():.0f}s (max {limit}s)"
        return self._set("disconnected", self._cfg.breakers.disconnected_level, why, "condition", view, now)

    def _check_drift(self, view: AccountView, now: datetime) -> RiskOutput:
        limit, offset = self._cfg.breakers.max_clock_drift_ms, self.drift.offset_ms()
        if limit is None or offset is None:
            return NOTHING
        if abs(offset) <= limit:
            return self._clear("clock_drift", view, now)
        why = f"clock offset vs broker {offset:.0f} ms (max {limit} ms)"
        return self._set("clock_drift", self._cfg.breakers.clock_drift_level, why, "condition", view, now)

    def _new_day(self, view: AccountView, now: datetime) -> RiskOutput:
        self._day = self.trading_day(now)
        self._day_start = view.account.equity if view.account is not None else None
        out = RiskOutput(changed=True)
        if self._triggers.get("daily_loss") is not None:
            out |= self._clear("daily_loss", view, now, clears=("new_day",))
        return out

    def _note_equity(self, account: AccountState | None, now: datetime) -> bool:
        if account is None:
            return False
        changed = False
        if self._day is None or self._day_start is None:
            self._day, self._day_start, changed = self.trading_day(now), account.equity, True
        if self._peak is None or account.equity > self._peak:
            self._peak, changed = account.equity, True
        return changed

    # ================================================================== triggers

    def _set(
        self,
        name: str,
        level: KillLevel,
        reason: str,
        clears: Clears,
        view: AccountView,
        now: datetime,
        *,
        force_event: bool = False,
    ) -> RiskOutput:
        if level is KillLevel.NONE:
            return NOTHING
        current = self._triggers.get(name)
        if current is not None and current.level is level:
            current.reason = reason  # keep the original 'since'
            return NOTHING
        previous = self.level
        self._triggers[name] = Trigger(level=level, reason=reason, since=now, clears=clears)
        _log.warning("risk_trigger", account=self._account_id, trigger=name, level=str(level), reason=reason)
        escalated = self.level.rank > previous.rank
        events = (self._state_event(previous, f"{name}: {reason}", now),) if escalated or force_event else ()
        return RiskOutput(events=events, changed=True) | self._enforce(view, now, escalated=escalated)

    def _clear(
        self, name: str, view: AccountView, now: datetime, *, clears: tuple[Clears, ...] = ("condition",)
    ) -> RiskOutput:
        current = self._triggers.get(name)
        if current is None or current.clears not in clears:
            return NOTHING
        previous = self.level
        del self._triggers[name]
        _log.info("risk_trigger_cleared", account=self._account_id, trigger=name)
        if self.level is previous:
            return RiskOutput(changed=True)
        return RiskOutput(events=(self._state_event(previous, f"{name} cleared", now),), changed=True)

    def _enforce(self, view: AccountView, now: datetime, *, escalated: bool) -> RiskOutput:
        level = self.level
        if level.rank < KillLevel.CANCEL_PENDING.rank:
            return NOTHING
        due = escalated or self._last_enforce is None or now - self._last_enforce >= ENFORCE_EVERY
        cancel = bool(view.open_orders)
        flatten = level is KillLevel.FLATTEN_ALL and bool(view.positions)
        if not due or not (cancel or flatten):
            return NOTHING
        self._last_enforce = now
        return RiskOutput(cancel_pending=cancel, flatten=flatten)

    def _state_event(self, previous: KillLevel, reason: str, now: datetime) -> RiskStateEvent:
        return RiskStateEvent(
            account_id=self._account_id,
            level=self.level,
            previous=previous,
            reason=reason,
            triggers=tuple(sorted(self._triggers)),
            ts=now,
        )

    def _summary(self) -> str:
        return "; ".join(f"{n}: {t.reason}" for n, t in sorted(self._triggers.items()))

    # ================================================================== persistence

    def dump(self) -> JsonObj:
        triggers: dict[str, JsonValue] = {
            name: {
                "level": t.level.value,
                "reason": t.reason,
                "since_ms": to_epoch_ms(t.since),
                "clears": t.clears,
            }
            for name, t in self._triggers.items()
        }
        return {
            "day": self._day.isoformat() if self._day else None,
            "day_start_equity": None if self._day_start is None else str(self._day_start),
            "peak_equity": None if self._peak is None else str(self._peak),
            "rejects": self._rejects,
            "triggers": triggers,
        }

    def load(self, data: JsonObj) -> None:
        if not data:
            return
        day, start, peak = data.get("day"), data.get("day_start_equity"), data.get("peak_equity")
        self._day = date.fromisoformat(day) if isinstance(day, str) else None
        self._day_start = Decimal(start) if isinstance(start, str) else None
        self._peak = Decimal(peak) if isinstance(peak, str) else None
        rejects = data.get("rejects")
        self._rejects = rejects if isinstance(rejects, int) else 0
        raw = data.get("triggers")
        self._triggers = {}
        if isinstance(raw, dict):
            for name, t in raw.items():
                if not isinstance(t, dict):
                    continue
                since, clears = t.get("since_ms"), t.get("clears")
                self._triggers[name] = Trigger(
                    level=KillLevel(str(t.get("level"))),
                    reason=str(t.get("reason") or ""),
                    since=from_epoch_ms(since) if isinstance(since, int) else self._started,
                    clears=clears if clears in ("manual", "condition", "new_day") else "manual",  # type: ignore[arg-type]
                )
        if self._triggers:
            _log.warning(
                "risk_restored",
                account=self._account_id,
                level=str(self.level),
                triggers=sorted(self._triggers),
            )
