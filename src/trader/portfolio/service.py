"""PortfolioService: one account's P&L, kept current from OMS events and ticks (SPEC §9, ADR 0010).

- Every deal goes into the ``FillLedger`` with its strategy: from the order (client id) or the position.
  The ledger is backfilled from ``broker.deal_history`` on start and after every reconnect, so deals done
  while we were disconnected are not lost (broker deals are the source of truth for realized P&L).
- Equity is sampled every ``equity_sample`` and on every fill (``equity-YYYY-MM-DD.jsonl``); the high-water
  mark and drawdown come with each sample.
- **Drift check:** computed equity vs broker equity -> ``AlertEvent("equity_drift")`` past the threshold.
  A broker balance that moved without a deal (after a grace period and a backfill) is a balance operation
  (deposit, withdrawal, fee): ``AlertEvent("balance_mismatch")`` and the baseline is reset.
- Listeners (trade journal, execution tracker) get the ledger entries, positions, ticks and orders in order.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from trader.domain.enums import ConnectionState, RiskDecision
from trader.domain.events import (
    AccountEvent,
    AlertEvent,
    ConnectionEvent,
    DomainEvent,
    FillEvent,
    OrderEvent,
    OrderRetryEvent,
    PositionEvent,
    RiskEvent,
    TickEvent,
)
from trader.domain.models import Fill, Order, Tick
from trader.domain.time import Clock
from trader.marketdata.bus import Bus, Overflow, Subscription, TickBus
from trader.observability.logging import get_logger
from trader.risk.gate import AccountView

from .book import EquityPoint, PortfolioBook
from .ledger import FillLedger, LedgerEntry, trading_day

_log = get_logger("trader.portfolio")
MANUAL = "manual"


class OmsPort(Protocol):
    events: Bus[DomainEvent]

    def view(self) -> AccountView: ...
    def order(self, client_order_id: str) -> Order | None: ...


class DealSource(Protocol):
    async def deal_history(self, start: datetime, end: datetime) -> list[Fill]: ...


class Listener:
    """Receives the account's activity in order (no-op defaults)."""

    def on_entry(self, entry: LedgerEntry) -> None: ...
    def on_replay(self, entry: LedgerEntry) -> None:
        """A deal loaded from the ledger at startup (rebuild state); default: same as a live one."""
        self.on_entry(entry)

    def on_position(self, event: PositionEvent) -> None: ...
    def on_tick(self, tick: Tick) -> None: ...
    def on_order(self, order: Order) -> None: ...
    def on_retry(self, event: OrderRetryEvent) -> None: ...
    def flush(self) -> None:
        """Persist (blocking; called off the event loop)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class PortfolioSettings:
    equity_sample: timedelta = timedelta(seconds=60)
    drift_pct: Decimal = Decimal("0.5")  # of broker equity
    drift_min: Decimal = Decimal("1")  # account currency: never alert below this
    balance_grace: timedelta = timedelta(seconds=10)  # deals and balance updates may arrive in any order
    backfill_lookback: timedelta = timedelta(days=1)  # on a fresh ledger
    timer: timedelta = timedelta(seconds=1)


class PortfolioService:
    def __init__(
        self,
        account_id: str,
        oms: OmsPort,
        broker: DealSource,
        clock: Clock,
        *,
        ticks: TickBus | None = None,
        data_dir: Path | None = None,
        day_offset_minutes: int = 0,
        settings: PortfolioSettings | None = None,
        listeners: Sequence[Listener] = (),
    ) -> None:
        self.account_id = account_id
        self._oms, self._broker, self._clock, self._ticks = oms, broker, clock, ticks
        self._s = settings or PortfolioSettings()
        self.dir = None if data_dir is None else data_dir / "portfolio" / account_id
        self.ledger = FillLedger(None if self.dir is None else self.dir / "fills.jsonl", day_offset_minutes)
        self.book = PortfolioBook(account_id, self.ledger)
        self.listeners = list(listeners)
        self.events: Bus[DomainEvent] = Bus()
        self.curve: list[EquityPoint] = []
        self.risk_counts: dict[date, dict[str, int]] = {}
        self._pos_strategy: dict[str, str] = {}
        self._equity_lines: list[tuple[date, str]] = []
        self._last_sample: datetime | None = None
        self._last_change: datetime | None = None  # last deal or balance change
        self._drifting = False
        self._tasks: list[asyncio.Task[None]] = []
        self._backfilling: asyncio.Task[None] | None = None
        self._oms_sub: Subscription[DomainEvent] | None = None

    # ================================================================== lifecycle

    async def start(self) -> None:
        loaded = await asyncio.to_thread(self.ledger.load)
        for e in self.ledger.entries():
            self._pos_strategy.setdefault(e.fill.position_id, e.strategy_id)
            for listener in self.listeners:
                listener.on_replay(e)
        self._refresh_positions()
        view = self._oms.view()
        if view.account is not None:
            self.book.on_account(view.account)
        now = self._clock.now()
        last = self.ledger.last_ts()
        await self.backfill(last - timedelta(minutes=1) if last else now - self._s.backfill_lookback)
        self._oms_sub = self._oms.events.subscribe(f"portfolio-oms-{self.account_id}")
        self._tasks.append(
            asyncio.create_task(self._consume_oms(self._oms_sub), name=f"portfolio-{self.account_id}")
        )
        if self._ticks is not None:
            sub = self._ticks.subscribe(
                f"portfolio-{self.account_id}",
                policy=Overflow.COALESCE,
                key=lambda e: (e.broker, e.tick.symbol),
            )
            self._tasks.append(
                asyncio.create_task(self._consume_ticks(sub), name=f"portfolio-ticks-{self.account_id}")
            )
        self._tasks.append(asyncio.create_task(self._timer(), name=f"portfolio-timer-{self.account_id}"))
        self.sample(now)
        _log.info("portfolio_started", account=self.account_id, ledger=loaded, deals=len(self.ledger))

    async def stop(self) -> None:
        """Apply what the OMS already published (a fill queued here must reach the ledger), then stop.
        ``App.stop`` disconnects the brokers first, so this sees every fill up to the disconnect."""
        for task in [*self._tasks, *([self._backfilling] if self._backfilling else [])]:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if self._oms_sub is not None:
            while (event := self._oms_sub.get_nowait()) is not None:
                try:
                    self.on_event(event)
                except Exception:
                    _log.exception("portfolio_event_failed", account=self.account_id)
            self._oms.events.unsubscribe(self._oms_sub)
            self._oms_sub = None
        self.sample(self._clock.now())
        await self.flush()

    async def backfill(self, since: datetime) -> int:
        """Pull broker deals since ``since`` into the ledger (idempotent). Returns how many were new."""
        try:
            fills = await self._broker.deal_history(since, self._clock.now() + timedelta(seconds=1))
        except Exception as exc:  # best effort: live fills keep flowing; the next reconnect retries
            _log.warning("portfolio_backfill_failed", account=self.account_id, error=repr(exc))
            return 0
        new = sum(1 for f in sorted(fills, key=lambda f: f.ts) if self._add(f) is not None)
        if new:
            _log.info("portfolio_backfilled", account=self.account_id, deals=new, since=since.isoformat())
        return new

    async def flush(self) -> None:
        lines = self.ledger.take_pending()
        equity, self._equity_lines = self._equity_lines, []
        await asyncio.to_thread(self._write, lines, equity)
        for listener in self.listeners:
            await asyncio.to_thread(listener.flush)

    def _write(self, lines: list[str], equity: list[tuple[date, str]]) -> None:
        self.ledger.append_lines(lines)
        if self.dir is None or not equity:
            return
        self.dir.mkdir(parents=True, exist_ok=True)
        by_day: dict[date, list[str]] = {}
        for day, line in equity:
            by_day.setdefault(day, []).append(line)
        for day, day_lines in by_day.items():
            with (self.dir / f"equity-{day.isoformat()}.jsonl").open("a", encoding="utf-8") as fh:
                fh.write("\n".join(day_lines) + "\n")

    # ================================================================== events

    async def _consume_oms(self, sub: Subscription[DomainEvent]) -> None:
        async for event in sub:
            try:
                self.on_event(event)
            except Exception:
                _log.exception("portfolio_event_failed", account=self.account_id)

    async def _consume_ticks(self, sub: Subscription[TickEvent]) -> None:
        async for event in sub:
            if event.broker == self.account_id:
                self.on_tick(event.tick)

    def on_event(self, event: DomainEvent) -> None:
        now = self._clock.now()
        if isinstance(event, FillEvent):
            if self._add(event.fill) is not None:
                self.sample(now)
        elif isinstance(event, PositionEvent):
            self._pos_strategy.setdefault(event.position.position_id, event.position.strategy_id)
            self._refresh_positions()
            for listener in self.listeners:
                listener.on_position(event)
        elif isinstance(event, AccountEvent):
            before = self.book.account.balance if self.book.account else None
            self.book.on_account(event.state)
            if before is not None and event.state.balance != before:
                self._last_change = now
            self._check_drift(now)
        elif isinstance(event, OrderEvent):
            for listener in self.listeners:
                listener.on_order(event.order)
        elif isinstance(event, OrderRetryEvent):
            for listener in self.listeners:
                listener.on_retry(event)
        elif isinstance(event, RiskEvent):
            counts = self.risk_counts.setdefault(trading_day(event.ts, self.ledger.day_offset), {})
            key = (
                f"{event.decision.value.lower()}:{event.rule}"
                if event.decision is RiskDecision.REJECTED
                else "approved"
            )
            counts[key] = counts.get(key, 0) + 1
        elif isinstance(event, ConnectionEvent) and event.state is ConnectionState.UP:
            last = self.ledger.last_ts()
            since = last - timedelta(minutes=1) if last else now - self._s.backfill_lookback
            if self._backfilling is None or self._backfilling.done():
                self._backfilling = asyncio.get_running_loop().create_task(self._backfill_quietly(since))

    async def _backfill_quietly(self, since: datetime) -> None:
        await self.backfill(since)

    def on_tick(self, tick: Tick) -> None:
        self.book.on_tick(tick)
        for listener in self.listeners:
            listener.on_tick(tick)

    def _add(self, fill: Fill) -> LedgerEntry | None:
        entry = self.ledger.add(fill, self._strategy_of(fill))
        if entry is None:
            return None
        self._pos_strategy.setdefault(fill.position_id, entry.strategy_id)
        self._last_change = self._clock.now()
        for listener in self.listeners:
            listener.on_entry(entry)
        return entry

    def _strategy_of(self, fill: Fill) -> str:
        if fill.client_order_id:
            order = self._oms.order(fill.client_order_id)
            if order is not None:
                return order.request.strategy_id
        known = self._pos_strategy.get(fill.position_id)
        if known:
            return known
        pos = next((p for p in self._oms.view().positions if p.position_id == fill.position_id), None)
        return pos.strategy_id if pos is not None else MANUAL

    def _refresh_positions(self) -> None:
        view = self._oms.view()
        self.book.set_positions(view.positions, view.instruments)
        for p in view.positions:
            self._pos_strategy.setdefault(p.position_id, p.strategy_id)

    # ================================================================== samples and checks

    async def _timer(self) -> None:
        while True:
            await asyncio.sleep(self._s.timer.total_seconds())
            now = self._clock.now()
            self._refresh_positions()
            if self._last_sample is None or now - self._last_sample >= self._s.equity_sample:
                self.sample(now)
            self._check_balance(now)
            if self.ledger_pending():
                await self.flush()

    def ledger_pending(self) -> bool:
        return bool(self._equity_lines) or self.ledger.has_pending

    def sample(self, now: datetime) -> EquityPoint | None:
        point = self.book.sample(now)
        if point is None:
            return None
        self.curve.append(point)
        if len(self.curve) > 20_000:
            del self.curve[: len(self.curve) - 20_000]
        self._last_sample = now
        self._equity_lines.append(
            (trading_day(now, self.ledger.day_offset), json.dumps(point.to_json(), separators=(",", ":")))
        )
        return point

    def _check_drift(self, now: datetime) -> None:
        point = self.book.sample(now)
        if point is None or point.broker_equity is None or point.unpriced:
            return
        gap = point.equity - point.broker_equity
        limit = max(self._s.drift_min, abs(point.broker_equity) * self._s.drift_pct / 100)
        if abs(gap) > limit and not self._drifting:
            self._drifting = True
            self._alert(
                "equity_drift",
                "warning",
                f"computed equity {point.equity:.2f} vs broker {point.broker_equity:.2f} "
                f"(gap {gap:.2f}, limit {limit:.2f})",
                now,
            )
        elif abs(gap) <= limit and self._drifting:
            self._drifting = False
            _log.info("equity_drift_cleared", account=self.account_id, gap=str(gap))

    def _check_balance(self, now: datetime) -> None:
        gap = self.book.balance_gap()
        if gap is None or abs(gap) <= Decimal("0.01") * max(1, len(self.ledger)):
            return
        if self._last_change is not None and now - self._last_change < self._s.balance_grace:
            return  # the deal (or the balance update) may still be on its way
        if self._backfilling is not None and not self._backfilling.done():
            return
        self._alert(
            "balance_mismatch",
            "warning",
            f"broker balance differs from the deals by {gap:.2f}: a deposit, withdrawal or fee, or a missed "
            "deal (a backfill runs after every reconnect); baseline reset",
            now,
        )
        self.book.rebase(now)

    def _alert(self, kind: str, severity: str, message: str, now: datetime) -> None:
        _log.warning("portfolio_alert", account=self.account_id, kind=kind, message=message)
        self.events.publish(
            AlertEvent(account_id=self.account_id, kind=kind, severity=severity, message=message, ts=now)
        )

    # ================================================================== queries

    def ledger_day(self, ts: datetime) -> date:
        return trading_day(ts, self.ledger.day_offset)

    def snapshot(self) -> EquityPoint | None:
        return self.curve[-1] if self.curve else None
