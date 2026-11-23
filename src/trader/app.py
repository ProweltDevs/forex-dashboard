"""Composition root (SPEC §15).

- ``build_account``: one account's broker adapter + RiskEngine + OrderManager + AccountRuntime. Every account
  always gets a ``RiskEngine``; ``ApproveAll`` is for tests only.
- ``App`` (``trader run``): market data, the data-source feeds for paper accounts, every trading account and
  the strategy runner. Startup: connect -> reconcile -> restore -> warm up bars -> start strategies.
  Shutdown: pause strategies -> let in-flight orders settle -> persist -> disconnect.
- ``kill_account`` (``trader kill``): the standalone kill switch.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Literal

from trader import __version__
from trader.analytics.books import AccountBooks, build_books, load_offline, params_hash
from trader.analytics.execution import ExecutionStats, percentile
from trader.analytics.journal import TradeRecord
from trader.analytics.statement import StatementCheck, compare, day_window
from trader.api.control import Control
from trader.api.server import ApiServer, api_token
from trader.brokers.base import Broker, MarketDataSource
from trader.brokers.feeds import BrokerFeed, SyntheticFeed
from trader.brokers.registry import BrokerDeps, BrokerRegistry, default_registry
from trader.config.risk import RiskConfig
from trader.config.settings import AccountConfig, AppConfig, StrategyConfig
from trader.domain.enums import KillLevel, OrderStatus
from trader.domain.errors import ConfigError
from trader.domain.jsontypes import JsonValue
from trader.domain.models import Order
from trader.domain.time import Clock
from trader.marketdata.bus import Bus
from trader.marketdata.service import MarketDataService
from trader.notify.notifier import Channel
from trader.notify.service import Notifications
from trader.observability.logging import get_logger
from trader.observability.metrics import Metrics, OpsMonitor
from trader.observability.ops import OpsServer
from trader.oms import AccountRuntime, JsonFileStore, OmsSettings, OrderManager
from trader.persistence.audit import AuditLog, audit_path
from trader.persistence.files import write_atomic
from trader.persistence.service import Persistence
from trader.risk.engine import RiskEngine
from trader.strategy.base import StrategyStatus
from trader.strategy.registry import resolve
from trader.strategy.runner import StrategyRunner, StrategySpec, StrategyStateStore

_log = get_logger("trader.app")


@dataclass(frozen=True, slots=True)
class AccountServices:
    account: AccountConfig
    broker: Broker
    risk: RiskEngine
    oms: OrderManager
    runtime: AccountRuntime


def oms_store_path(data_dir: Path, account_id: str) -> Path:
    """One JSON snapshot per account: orders, attribution, emulations and the risk state (kill switch)."""
    return data_dir / "oms" / f"{account_id}.json"


def build_account(
    config: AppConfig,
    account: AccountConfig,
    *,
    env: Mapping[str, str],
    clock: Clock,
    registry: BrokerRegistry | None = None,
    market_data: MarketDataSource | None = None,
    oms_settings: OmsSettings | None = None,
    md_service: MarketDataService | None = None,
    warmup_bars: int | None = None,
) -> AccountServices:
    deps = BrokerDeps(clock=clock, env=env, data_dir=config.data_dir, market_data=market_data)
    broker = (registry or default_registry()).create(account, deps)
    risk = RiskEngine(account.risk, clock, account_id=account.id)
    store = JsonFileStore(oms_store_path(config.data_dir, account.id))
    oms = OrderManager(broker, risk, store, clock, oms_settings or OmsSettings(account_id=account.id))
    runtime = AccountRuntime(
        account.id, broker, oms, symbols=account.symbols, market_data=md_service, warmup_bars=warmup_bars
    )
    return AccountServices(account=account, broker=broker, risk=risk, oms=oms, runtime=runtime)


@dataclass(frozen=True, slots=True)
class KillResult:
    level: KillLevel
    positions_left: int
    orders_left: int
    done: bool  # the level's actions completed within the timeout


async def kill_account(
    services: AccountServices,
    level: KillLevel | None,
    reason: str,
    *,
    timeout: timedelta = timedelta(seconds=60),
    out: Callable[[str], object] = print,
) -> KillResult:
    """Standalone kill switch: connect, reconcile, set ``level`` (``None`` = reset), wait until pending
    orders are cancelled (CANCEL_PENDING) and positions closed (FLATTEN_ALL), persist, disconnect.

    The level is stored in the account's OMS snapshot, so the trading process restores it on start. Do not
    run this while another process manages the same account: both would write the snapshot.
    """
    if services.account.broker == "paper":
        raise ConfigError("paper accounts exist only inside the trading process; there is nothing to kill")
    runtime, oms = services.runtime, services.oms
    await runtime.start(warm_up=False)
    try:
        before = oms.view()
        name = services.account.id
        out(f"{name}: {len(before.positions)} positions, {len(before.open_orders)} pending orders")
        if level is None:
            new = await oms.reset_kill_switch(reason)
        else:
            new = await oms.kill_switch(level, reason)
        out(f"{services.account.id}: kill switch {new}")
        done = await _wait_done(oms, new, timeout)
        await oms.flush()
        view = oms.view()
        return KillResult(
            level=new, positions_left=len(view.positions), orders_left=len(view.open_orders), done=done
        )
    finally:
        await runtime.stop()


async def _wait_done(oms: OrderManager, level: KillLevel, timeout: timedelta) -> bool:
    def done() -> bool:
        view = oms.view()
        if level.rank >= KillLevel.FLATTEN_ALL.rank and view.positions:
            return False
        return not (level.rank >= KillLevel.CANCEL_PENDING.rank and view.open_orders)

    try:
        async with asyncio.timeout(timeout.total_seconds()):
            while not done():
                await asyncio.sleep(0.5)
    except TimeoutError:
        return False
    return True


def risk_summary(risk: RiskConfig) -> str:
    """One line for the live startup banner."""
    lim, b = risk.limits, risk.breakers

    def show(value: object, unit: str = "") -> str:
        return "off" if value is None else f"{value}{unit}"

    return (
        f"max order {show(lim.max_order_volume, ' lots')}, loss/trade {show(lim.max_loss_per_trade_pct, '%')}"
        f" (SL {'required' if lim.require_sl else 'optional'}), daily loss {show(lim.daily_loss_pct, '%')}"
        f" -> {b.daily_loss_level}, drawdown {show(lim.max_drawdown_pct, '%')} -> {b.drawdown_level},"
        f" margin {show(lim.max_margin_usage_pct, '%')}, {len(risk.strategies)} strategy overrides"
    )


# ================================================================================================ trader run


@dataclass(slots=True)
class RunReport:
    started: datetime
    stopped: datetime | None
    strategies: list[dict[str, JsonValue]]
    accounts: list[dict[str, JsonValue]]

    @property
    def strategy_errors(self) -> int:
        return sum(int(str(s["total_errors"])) for s in self.strategies)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "started": self.started.isoformat(),
            "stopped": self.stopped.isoformat() if self.stopped else None,
            "strategy_errors": self.strategy_errors,
            "strategies": list(self.strategies),
            "accounts": list(self.accounts),
        }

    def lines(self) -> list[str]:
        out = [f"run {self.started.isoformat()} -> {self.stopped.isoformat() if self.stopped else '...'}"]
        for st in self.strategies:
            out.append(
                f"  strategy {st['id']}: {st['status']} callbacks={st['callbacks']} intents={st['intents']} "
                f"errors={st['total_errors']} slow={st['slow']}"
                + (f" ({st['pause_reason']})" if st["pause_reason"] else "")
            )
        for acc in self.accounts:
            out.append(
                f"  account {acc['id']}: kill={acc['kill_level']} positions={acc['positions']} "
                f"realized_today={acc.get('realized_today')} trades_closed={acc.get('trades_closed')} "
                f"equity={acc.get('equity')} drawdown={acc.get('drawdown')}"
            )
            out.append(f"    orders by status: {acc['orders']}")
        out.append(f"  strategy errors: {self.strategy_errors}")
        return out


class App:
    """The long-running process: every configured account plus the strategy runner."""

    def __init__(
        self,
        config: AppConfig,
        *,
        env: Mapping[str, str],
        clock: Clock,
        registry: BrokerRegistry | None = None,
        settle_timeout: timedelta = timedelta(seconds=10),
        notify_channels: list[Channel] | None = None,  # replaces the configured channels (tests)
    ) -> None:
        self.config = config
        self._env = env
        self._clock = clock
        self.clock = clock
        self._registry = registry or default_registry()
        self._settle_timeout = settle_timeout
        self.market = MarketDataService(config.market_data, clock, config.data_dir)
        self.feeds: dict[str, BrokerFeed | SyntheticFeed] = {}
        self.accounts: dict[str, AccountServices] = {}
        self.runner: StrategyRunner | None = None
        self.persistence = Persistence.from_config(config, dict(env), clock)  # None without a database
        self.books: dict[str, AccountBooks] = {}
        self.audit_log = AuditLog(audit_path(config.data_dir), clock, self.persistence)
        self.strategy_configs: dict[str, StrategyConfig] = {st.id: st for st in config.strategies}
        self.notifications = Notifications.from_config(config, env, clock, channels=notify_channels)
        self.created = clock.now()
        self.state: Literal["created", "starting", "running", "stopping", "stopped"] = "created"
        self.metrics = Metrics()
        self.metrics.build.info({"version": __version__, "mode": str(config.mode)})
        self.metrics.started.set(self.created.timestamp())
        self.monitor = OpsMonitor(
            self,
            self.metrics,
            clock,
            sample=timedelta(seconds=config.ops.sample_s),
            drift_alert=timedelta(seconds=config.ops.clock_drift_alert_s),
        )
        self.ops_events = self.monitor.events  # ops alerts (clock drift)
        self.ops = OpsServer(self, config.ops) if config.ops.enabled else None
        self.api: ApiServer | None = None
        if config.api.enabled:
            self.api = ApiServer(self, config.api, api_token(config.api, env))
        self._report_day: dict[str, date] = {}
        self._reporter: asyncio.Task[None] | None = None
        self._started: datetime | None = None

    def _sources(self) -> set[str]:
        return {
            a.paper.market_data_from for a in self.config.accounts if a.paper and a.paper.market_data_from
        }

    def _warmup(self, account: AccountConfig) -> int | None:
        """Bars to load per timeframe: the most any strategy on the account needs (None = config default)."""
        needs = [
            st.warmup_bars if st.warmup_bars is not None else resolve(st.class_).warmup_bars
            for st in self.config.strategies
            if st.enabled and st.account == account.id
        ]
        return max([self.config.market_data.warmup_bars, *needs]) if needs else None

    async def start(self) -> None:
        self._started = self._clock.now()
        self.state = "starting"
        if self.ops is not None:
            await self.ops.start()  # first: liveness answers during a long startup, readiness says "starting"
        db = self.persistence
        if db is not None:
            await db.start()  # never blocks startup: an unreachable database only means WAL buffering
            for acc in self.config.accounts:
                db.put(
                    "account",
                    acc.id,
                    {
                        "broker": acc.broker,
                        "environment": acc.environment,
                        "broker_account": acc.account_id or None,
                    },
                )
        notes = self.notifications
        if notes is not None:
            await notes.start()
            notes.set_order_lookup(self._order_of)
        await self.market.start()
        sources = self._sources()
        by_id = {a.id: a for a in self.config.accounts}
        for source_id in sorted(sources):
            acc = by_id[source_id]
            deps = BrokerDeps(clock=self._clock, env=self._env, data_dir=self.config.data_dir)
            feed = BrokerFeed(self._registry.create(acc, deps))
            await feed.start()
            self.feeds[source_id] = feed
            _log.info("data_source_ready", account=source_id, broker=acc.broker)
        for acc in self.config.accounts:
            if acc.id in sources:
                continue
            source: BrokerFeed | SyntheticFeed | None = None
            if acc.broker == "paper":
                source = await self._paper_source(acc)
            services = build_account(
                self.config,
                acc,
                env=self._env,
                clock=self._clock,
                registry=self._registry,
                market_data=source,
                md_service=self.market,
                warmup_bars=self._warmup(acc),
            )
            await services.runtime.start(warm_up=True)
            self.accounts[acc.id] = services
            books = build_books(
                self.config,
                acc,
                services.oms,
                services.broker,
                services.broker.name,
                self._clock,
                ticks=self.market.ticks,
            )
            await asyncio.to_thread(books.journal.load)  # closed trades first: the ledger replay skips them
            await books.portfolio.start()
            self.books[acc.id] = books
            if notes is not None:
                notes.attach(services.oms.events, acc.id)
                notes.attach(books.portfolio.events, f"{acc.id}-alerts")
            if db is not None:
                db.attach(services.oms.events, acc.id)
                db.attach(books.portfolio.events, acc.id, name="persistence-alerts")
                for inst in services.oms.view().instruments.values():
                    db.put("instrument", acc.id, inst)
                books.journal.on_closed.append(partial(self._persist_trade, acc.id))
            self._report_day[acc.id] = books.portfolio.ledger_day(self._clock.now())
        self.runner = StrategyRunner(
            self.market,
            {aid: s.oms for aid, s in self.accounts.items()},
            self._clock,
            StrategyStateStore(self.config.data_dir / "strategies"),
        )
        for st in self.config.strategies:
            if st.enabled:
                self.runner.add(self._spec(st))
        if db is not None:
            db.attach(self.runner.events, None, name="persistence-strategies")
            self.runner.on_state_saved.append(self._persist_state)
        if notes is not None:
            notes.attach(self.runner.events, "strategies")
            notes.attach(self.market.feeds, "feeds")
            notes.attach(self.ops_events, "ops")
            if db is not None:
                notes.attach(db.writer.events, "db")
        if db is not None:
            db.attach(self.ops_events, None, name="persistence-ops")
            db.attach(self.market.feeds, None, name="persistence-feeds")  # stale/live: domain events too
        self._attach_metrics()
        await self.monitor.start()
        await self.runner.run()
        await self.runner.start_all()
        self._persist_strategies()
        self._reporter = asyncio.create_task(self._daily_reports(), name="daily-reports")
        if self.api is not None:
            await self.api.start()  # last: the API only ever sees a fully started app
        if notes is not None:
            tg = self.config.notify.telegram
            if tg is not None and tg.commands:
                await notes.enable_commands(Control(self, actor="telegram"))
            notes.info("app:started", "Trader started", "\n".join(self._status_lines()))
        self.state = "running"
        _log.info("app_started", accounts=sorted(self.accounts), strategies=sorted(self.runner.handles))

    def _attach_metrics(self) -> None:
        mon = self.monitor
        mon.attach(self.market.ticks, "ticks", maxsize=200_000)
        mon.attach(self.market.feeds, "feeds")
        for aid, services in self.accounts.items():
            mon.attach(services.oms.events, aid)
        for aid, books in self.books.items():
            mon.attach(books.portfolio.events, f"{aid}-alerts")
        if self.runner is not None:
            mon.attach(self.runner.events, "strategies")
        if self.persistence is not None:
            mon.attach(self.persistence.writer.events, "db")

    def event_buses(self) -> list[Bus[Any]]:
        """Every event bus of the process (queue-depth metrics)."""
        buses: list[Bus[Any]] = [self.market.ticks, self.market.bars, self.market.feeds, self.ops_events]
        buses += [s.oms.events for s in self.accounts.values()]
        buses += [b.portfolio.events for b in self.books.values()]
        if self.runner is not None:
            buses.append(self.runner.events)
        if self.persistence is not None:
            buses.append(self.persistence.writer.events)
        if self.api is not None and self.api.hub is not None:
            buses.append(self.api.hub.events)
        return buses

    def _order_of(self, account_id: str, client_order_id: str) -> Order | None:
        services = self.accounts.get(account_id)
        return None if services is None else services.oms.order(client_order_id)

    def _status_lines(self) -> list[str]:
        lines = [
            f"{aid} ({s.account.broker} {s.account.environment}): kill {s.oms.risk_level}, "
            f"{len(s.oms.positions())} positions"
            for aid, s in self.accounts.items()
        ]
        if self.runner is not None:
            lines.append(
                "strategies: "
                + (", ".join(f"{sid} {status}" for sid, status, _r, _st in self.runner.status()) or "none")
            )
        return lines

    def _spec(self, st: StrategyConfig) -> StrategySpec:
        return StrategySpec(
            id=st.id,
            cls=resolve(st.class_),
            params=st.params,
            account=st.account,
            symbols=st.symbols,
            timeframes=st.timeframes or self.config.market_data.timeframes[:1],
            max_errors=st.max_errors,
            callback_budget_ms=st.callback_budget_ms,
            seed=st.seed,
        )

    # ------------------------------------------------------------------ strategy management (Control API)

    async def add_strategy(self, st: StrategyConfig, *, start: bool = True) -> None:
        """Load a strategy that is not in the config file (it lasts until the process stops)."""
        runner = self.runner
        assert runner is not None
        if st.account not in self.accounts:
            raise ValueError(f"strategy {st.id!r}: unknown trading account {st.account!r}")
        if st.id in runner.handles and runner.handles[st.id].status is not StrategyStatus.STOPPED:
            raise ValueError(f"strategy {st.id!r} is already loaded")
        missing = sorted(set(st.symbols) - set(self.accounts[st.account].account.symbols))
        if missing:
            raise ValueError(f"strategy {st.id!r}: {st.account} does not subscribe to {missing}")
        runner.add(self._spec(st))
        self.strategy_configs[st.id] = st
        if start:
            await runner.start(st.id)
        self._persist_strategies()

    async def restart_strategy(self, strategy_id: str) -> None:
        """A STOPPED strategy as a new instance (its saved state carries over)."""
        runner = self.runner
        assert runner is not None
        runner.add(self._spec(self.strategy_configs[strategy_id]))
        await runner.start(strategy_id)
        self._persist_strategies()

    async def replace_strategy_params(self, strategy_id: str, params: dict[str, object]) -> None:
        runner = self.runner
        assert runner is not None
        handle = runner.handles[strategy_id]
        was, reason, by_risk = handle.status, handle.pause_reason, handle.paused_by_risk
        st = self.strategy_configs[strategy_id].model_copy(update={"params": params})
        if was is StrategyStatus.STOPPED:
            runner.add(self._spec(st))  # LOADED with the new params; start it explicitly
        else:
            await runner.replace(strategy_id, params)
            if was is StrategyStatus.PAUSED and not by_risk:
                runner.pause(strategy_id, reason or "paused")
        self.strategy_configs[strategy_id] = st
        self._persist_strategies()

    async def _paper_source(self, acc: AccountConfig) -> BrokerFeed | SyntheticFeed:
        assert acc.paper is not None
        if acc.paper.market_data_from is not None:
            return self.feeds[acc.paper.market_data_from]
        cfg = acc.paper.synthetic_feed
        if cfg is None:
            raise ConfigError(f"paper account {acc.id} needs paper.market_data_from or paper.synthetic_feed")
        _log.warning(
            "synthetic_feed", account=acc.id, detail="random-walk prices: not market data, not recorded"
        )
        self.market.exclude_from_recording(acc.id)
        feed = SyntheticFeed(
            acc.symbols,
            self._clock,
            seed=cfg.seed,
            interval=timedelta(milliseconds=cfg.tick_interval_ms),
            volatility=cfg.volatility,
        )
        self.feeds[f"synthetic:{acc.id}"] = feed
        await feed.start()
        return feed

    async def stop(self) -> RunReport:
        """Pause strategies, let in-flight orders settle, persist everything, disconnect."""
        self.state = "stopping"
        if self.api is not None:
            await self.api.stop()  # first: no new commands during shutdown
        runner = self.runner
        if runner is not None:
            for sid in list(runner.handles):
                runner.pause(sid, "shutdown")
        await self._settle()
        report = self.report()
        if runner is not None:
            await runner.stop_all()
            await runner.close()
        if self._reporter is not None:
            self._reporter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reporter
        for services in self.accounts.values():
            await services.runtime.stop()  # first: the books then see every fill up to the disconnect
        for aid, books in self.books.items():
            await books.portfolio.stop()
            await books.write_report(self.config.data_dir, self._report_day[aid])  # today so far
        for feed in self.feeds.values():
            await feed.close()
        await self.market.stop()
        await self.monitor.stop()
        if self.notifications is not None:
            self.notifications.info(
                "app:stopped",
                "Trader stopped",
                "\n".join([*self._status_lines(), f"strategy errors this run: {report.strategy_errors}"]),
            )
            await self.notifications.stop()
        if self.persistence is not None:
            self._persist_strategies()
            await self.persistence.stop()  # last: flushes to the database, or to the WAL if it is down
        report.stopped = self._clock.now()
        self.state = "stopped"
        if self.ops is not None:
            await self.ops.stop()  # last: readiness said "stopping" until the end
        _log.info("app_stopped", strategy_errors=report.strategy_errors)
        return report

    async def _settle(self) -> None:
        """Wait (bounded) until no order is still being submitted, so nothing is left unrecorded."""
        in_flight = (OrderStatus.NEW, OrderStatus.PENDING_SUBMIT, OrderStatus.UNKNOWN)
        try:
            async with asyncio.timeout(self._settle_timeout.total_seconds()):
                while any(
                    o.status in in_flight for s in self.accounts.values() for o in s.oms.view().open_orders
                ):
                    await asyncio.sleep(0.1)
        except TimeoutError:
            _log.warning(
                "shutdown_orders_still_in_flight", detail="they stay persisted and resolve on restart"
            )

    def _persist_trade(self, account_id: str, trade: TradeRecord) -> None:
        if self.persistence is not None:
            self.persistence.put("trade", account_id, trade.to_json())

    def _persist_state(self, strategy_id: str, text: str) -> None:
        if self.persistence is not None:
            self.persistence.put(
                "strategy_state", None, {"strategy_id": strategy_id, "state": json.loads(text)}
            )

    def _persist_strategies(self) -> None:
        if self.persistence is None or self.runner is None:
            return
        for sid, status, _reason, _stats in self.runner.status():
            st = self.strategy_configs[sid]
            cls = resolve(st.class_)
            self.persistence.put(
                "strategy",
                st.account,
                {
                    "strategy_id": sid,
                    "class_name": f"{cls.__module__}:{cls.__name__}",
                    "version": cls.version,
                    "params": dict(st.params),
                    "params_hash": params_hash(st.params),
                    "status": str(status),
                },
            )

    async def _daily_reports(self) -> None:
        """At each trading-day rollover, write the finished day's report for every account."""
        while True:
            await asyncio.sleep(30)
            now = self._clock.now()
            for aid, books in self.books.items():
                today = books.portfolio.ledger_day(now)
                if today != self._report_day[aid]:
                    await books.portfolio.flush()
                    paths = await books.write_report(self.config.data_dir, self._report_day[aid])
                    _log.info(
                        "daily_report", account=aid, day=self._report_day[aid].isoformat(), files=len(paths)
                    )
                    if self.notifications is not None:
                        self.notifications.daily_report(
                            aid, self._report_day[aid], daily_lines(books, self._report_day[aid])
                        )
                    self._report_day[aid] = today

    def report(self) -> RunReport:
        strategies: list[dict[str, JsonValue]] = []
        if self.runner is not None:
            for sid, status, reason, stats in self.runner.status():
                strategies.append(
                    {
                        "id": sid,
                        "status": str(status),
                        "pause_reason": reason,
                        "callbacks": stats.callbacks,
                        "intents": stats.intents,
                        "total_errors": stats.total_errors,
                        "slow": stats.slow,
                        "rejected_commands": stats.rejected_commands,
                        "last_error": stats.last_error,
                    }
                )
        accounts: list[dict[str, JsonValue]] = []
        for aid, s in self.accounts.items():
            by_status: dict[str, int] = {}
            for o in s.oms.orders():
                by_status[str(o.status)] = by_status.get(str(o.status), 0) + 1
            entry: dict[str, JsonValue] = {
                "id": aid,
                "kill_level": str(s.oms.risk_level),
                "positions": len(s.oms.positions()),
                "orders": dict(sorted(by_status.items())),
            }
            books = self.books.get(aid)
            if books is not None:
                p = books.portfolio
                point = p.sample(self._clock.now())
                today = p.ledger_day(self._clock.now())
                entry |= {
                    "realized_today": str(p.ledger.realized(day=today).net),
                    "trades_closed": len(books.journal.closed),
                    "equity": None if point is None else str(point.equity),
                    "drawdown": None if point is None else str(point.drawdown),
                }
            accounts.append(entry)
        return RunReport(
            started=self._started or self._clock.now(), stopped=None, strategies=strategies, accounts=accounts
        )


def daily_lines(books: AccountBooks, day: date) -> list[str]:
    """The daily-report notification: realized P&L, closed trades, equity."""
    p = books.portfolio
    pnl = p.ledger.realized(day=day)
    trades = [t for t in books.journal.trades() if p.ledger_day(t.exit_time) == day]
    wins = sum(1 for t in trades if t.net > 0)
    lines = [
        f"realized {pnl.net:.2f} (gross {pnl.gross:.2f}, costs {pnl.costs:.2f}, {pnl.deals} deals)",
        f"trades closed {len(trades)}, won {wins}" + (f" ({wins * 100 // len(trades)}%)" if trades else ""),
    ]
    for (sid, sym), v in sorted(p.ledger.attribution(day).items()):
        lines.append(f"  {sid} {sym}: {v.net:.2f}")
    point = p.snapshot()
    if point is not None:
        lines.append(f"equity {point.equity:.2f}, drawdown {point.drawdown:.2f} ({point.drawdown_pct:.2f}%)")
    risk = p.risk_counts.get(day, {})
    if risk:
        lines.append("risk events: " + ", ".join(f"{k} {v}" for k, v in sorted(risk.items())))
    st = ExecutionStats()
    for r in books.executions.records:
        if (ts := r.send_ts or r.signal_ts) is not None and p.ledger_day(ts) == day:
            st.add(r)
    if st.orders:
        slip = sum(st.slippage_points) / len(st.slippage_points) if st.slippage_points else None
        ack = percentile(st.latencies["send_to_ack"], 95)
        lines.append(
            f"execution: {st.orders} orders, filled {st.filled}, rejected {st.rejected}, "
            f"requoted {st.requoted}"
            + ("" if slip is None else f", slippage {slip:+.1f} pts avg")
            + ("" if ack is None else f", ack p95 {ack:.0f} ms")
        )
    return lines


async def run_app(app: App, stop: asyncio.Event, duration: timedelta | None = None) -> RunReport:
    """Start, run until ``stop`` is set (signal) or ``duration`` elapses, then shut down gracefully."""
    try:
        await app.start()
    except BaseException:  # e.g. the API port is taken: stop whatever already started, gracefully
        await app.stop()
        raise
    try:
        timeout = duration.total_seconds() if duration is not None else None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout)
    finally:
        report = await app.stop()
    reports = app.config.data_dir / "reports"
    stamp = report.started.strftime("%Y%m%dT%H%M%SZ")
    await asyncio.to_thread(
        write_atomic, reports / f"run-{stamp}.json", json.dumps(report.to_json(), indent=2)
    )
    return report


async def run_pnl_check(
    config: AppConfig,
    account: AccountConfig,
    day: date,
    *,
    env: Mapping[str, str],
    clock: Clock,
    registry: BrokerRegistry | None = None,
) -> StatementCheck:
    """``trader pnl-check``: the broker's deals for ``day`` vs our stored ledger. Read-only: connects,
    fetches the deal history and disconnects; never sends an order."""
    deps = BrokerDeps(clock=clock, env=env, data_dir=config.data_dir)
    broker = (registry or default_registry()).create(account, deps)
    await broker.connect()
    try:
        broker_fills = await broker.deal_history(*day_window(day, account.risk.day_offset_minutes))
    finally:
        await broker.close()
    ledger = (await asyncio.to_thread(load_offline, config.data_dir, account)).ledger
    return compare(day, [e.fill for e in ledger.entries(day=day)], broker_fills)


async def audit(
    config: AppConfig,
    env: Mapping[str, str],
    clock: Clock,
    *,
    actor: str,
    action: str,
    account_id: str | None,
    details: Mapping[str, object],
    result: str,
) -> None:
    """Record a manual command (SPEC §12): in the local audit file always, and in ``audit_log`` when a
    database is configured (with the database down it lands in the WAL and reaches the database with the
    next writer start)."""
    persistence = Persistence.from_config(config, dict(env), clock)
    if persistence is not None:
        await persistence.start()
    try:
        await AuditLog(audit_path(config.data_dir), clock, persistence).record(
            actor=actor, action=action, account_id=account_id, details=details, result=result
        )
    finally:
        if persistence is not None:
            await persistence.stop()
