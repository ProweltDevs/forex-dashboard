"""Control: the operations the Control API, the CLI and the Telegram bot perform on a running ``App``.

Rules it keeps (CLAUDE.md):
- Manual orders follow the one order path: an ``OrderIntent`` with ``strategy_id="manual"`` through
  ``OrderManager.submit``, so the RiskEngine checks them like any strategy's (``risk.strategies.manual``
  can set limits for them). Closing a position is risk-reducing and always allowed.
- Every mutating call is audited (``AuditLog``): actor, action, details, result, including failures.
- Nothing here writes order state: the OMS does.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from trader.analytics import execution, metrics
from trader.analytics.execution import ExecutionRecord
from trader.analytics.journal import TradeRecord
from trader.config.settings import StrategyConfig
from trader.domain.enums import KillLevel, OrderStatus
from trader.domain.models import Order, OrderIntent, Position
from trader.persistence.audit import AuditEntry
from trader.strategy.base import StrategyStatus
from trader.strategy.registry import BUILTIN

from . import schemas as s

if TYPE_CHECKING:
    from trader.app import AccountServices, App

MANUAL = "manual"  # strategy_id of orders placed by hand (API, CLI, bot)

_OPEN = frozenset(
    {
        OrderStatus.NEW,
        OrderStatus.PENDING_SUBMIT,
        OrderStatus.ACCEPTED,
        OrderStatus.WORKING,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.UNKNOWN,
    }
)


class NotFound(LookupError):
    pass


class Conflict(RuntimeError):
    """The request is valid but cannot be done in the current state."""


class Control:
    def __init__(self, app: App, *, actor: str = "api") -> None:
        self.app = app
        self.actor = actor

    # ================================================================== helpers

    def _account(self, account_id: str) -> AccountServices:
        services = self.app.accounts.get(account_id)
        if services is None:
            raise NotFound(f"unknown account {account_id!r}")
        return services

    def _accounts(self, account_id: str | None) -> list[AccountServices]:
        if account_id is None:
            return list(self.app.accounts.values())
        return [self._account(account_id)]

    async def _audited[T](
        self,
        action: str,
        account_id: str | None,
        details: Mapping[str, object],
        run: Callable[[], Awaitable[T]],
        describe: Callable[[T], str] = lambda r: "ok",
    ) -> T:
        try:
            result = await run()
        except Exception as exc:
            await self.app.audit_log.record(
                actor=self.actor,
                action=action,
                account_id=account_id,
                details=details,
                result=f"error: {type(exc).__name__}: {exc}",
            )
            raise
        await self.app.audit_log.record(
            actor=self.actor, action=action, account_id=account_id, details=details, result=describe(result)
        )
        return result

    # ================================================================== read

    def health(self) -> s.HealthOut:
        accounts = list(self.app.accounts.values())
        up = sum(1 for a in accounts if a.oms.view().broker_connected and a.oms.ready.is_set())
        return s.HealthOut(
            status="ok" if accounts and up == len(accounts) else "degraded",
            accounts_connected=up,
            accounts=len(accounts),
        )

    def accounts(self) -> list[s.AccountOut]:
        return [
            s.AccountOut(
                id=aid,
                broker=a.account.broker,
                environment=a.account.environment,
                connected=a.oms.view().broker_connected,
                ready=a.oms.ready.is_set(),
                kill_level=a.oms.risk_level,
            )
            for aid, a in self.app.accounts.items()
        ]

    def account_state(self, account_id: str) -> s.AccountStateOut:
        services = self._account(account_id)
        view = services.oms.view()
        acct = view.account
        books = self.app.books.get(account_id)
        point = None if books is None else books.portfolio.book.sample(self.app.clock.now())
        today = None
        if books is not None:
            today = books.portfolio.ledger.realized(day=books.portfolio.ledger_day(self.app.clock.now())).net
        return s.AccountStateOut(
            id=account_id,
            currency=acct.currency if acct else None,
            balance=acct.balance if acct else None,
            equity=acct.equity if acct else None,
            margin=acct.margin if acct else None,
            free_margin=acct.free_margin if acct else None,
            computed_equity=point.equity if point else None,
            unrealized=point.unrealized if point else None,
            realized_today=today,
            high_water=point.high_water if point else None,
            drawdown=point.drawdown if point else None,
            positions=len(view.positions),
            open_orders=len(view.open_orders),
            kill_level=services.oms.risk_level,
            risk_triggers={name: f"{t.level}: {t.reason}" for name, t in services.risk.triggers.items()},
        )

    def _position_out(self, account_id: str, pos: Position) -> s.PositionOut:
        books = self.app.books.get(account_id)
        return s.PositionOut(
            account_id=account_id,
            position_id=pos.position_id,
            symbol=pos.symbol,
            side=pos.side,
            volume=pos.volume,
            entry_price=pos.entry_price,
            sl=pos.sl,
            tp=pos.tp,
            strategy_id=pos.strategy_id,
            opened_ts=pos.opened_ts,
            unrealized=None if books is None else books.portfolio.book.unrealized_of(pos),
        )

    def positions(self, account_id: str | None = None, symbol: str | None = None) -> list[s.PositionOut]:
        return [
            self._position_out(a.account.id, p)
            for a in self._accounts(account_id)
            for p in a.oms.positions()
            if symbol is None or p.symbol == symbol
        ]

    @staticmethod
    def _order_out(account_id: str, o: Order) -> s.OrderOut:
        r = o.request
        return s.OrderOut(
            account_id=account_id,
            client_order_id=r.client_order_id,
            broker_order_id=o.broker_order_id,
            strategy_id=r.strategy_id,
            symbol=r.symbol,
            side=r.side,
            order_type=r.order_type,
            volume=r.volume,
            filled_volume=o.filled_volume,
            avg_fill_price=o.avg_fill_price,
            limit_price=r.limit_price,
            stop_price=r.stop_price,
            sl=r.sl,
            tp=r.tp,
            status=str(o.status),
            rejection_reason=o.rejection_reason,
            created=o.history[0].ts,
            updated=o.history[-1].ts,
        )

    def orders(
        self,
        account_id: str | None = None,
        *,
        open_only: bool = False,
        strategy_id: str | None = None,
        limit: int = 200,
    ) -> list[s.OrderOut]:
        out = [
            self._order_out(a.account.id, o)
            for a in self._accounts(account_id)
            for o in a.oms.orders()
            if (not open_only or o.status in _OPEN)
            and (strategy_id is None or o.request.strategy_id == strategy_id)
        ]
        out.sort(key=lambda o: o.updated, reverse=True)
        return out[:limit]

    def order(self, client_order_id: str) -> s.OrderOut:
        for aid, a in self.app.accounts.items():
            o = a.oms.order(client_order_id)
            if o is not None:
                return self._order_out(aid, o)
        raise NotFound(f"unknown order {client_order_id!r}")

    def strategies(self) -> list[s.StrategyOut]:
        runner = self.app.runner
        if runner is None:
            return []
        out = []
        for sid, h in runner.handles.items():
            spec, st = h.spec, h.stats
            out.append(
                s.StrategyOut(
                    id=sid,
                    account=spec.account,
                    class_name=f"{spec.cls.__module__}:{spec.cls.__name__}",
                    version=spec.cls.version,
                    status=str(h.status),
                    pause_reason=h.pause_reason,
                    params=h.ctx.params.model_dump(mode="json"),
                    symbols=list(spec.symbols),
                    timeframes=[str(tf) for tf in spec.timeframes],
                    callbacks=st.callbacks,
                    intents=st.intents,
                    errors=st.total_errors,
                    slow_callbacks=st.slow,
                    last_error=st.last_error,
                )
            )
        return out

    def strategy(self, strategy_id: str) -> s.StrategyOut:
        for st in self.strategies():
            if st.id == strategy_id:
                return st
        raise NotFound(f"unknown strategy {strategy_id!r}")

    def risk(self, account_id: str | None = None) -> list[s.RiskAccountOut]:
        return [
            s.RiskAccountOut(
                account_id=a.account.id,
                level=a.oms.risk_level,
                triggers={name: f"{t.level}: {t.reason}" for name, t in a.risk.triggers.items()},
                day_start_equity=a.risk.day_start_equity,
                peak_equity=a.risk.peak_equity,
                limits=a.account.risk.limits.model_dump(mode="json"),
                breakers=a.account.risk.breakers.model_dump(mode="json"),
            )
            for a in self._accounts(account_id)
        ]

    def pnl(self, account_id: str, day: date | None = None) -> s.PnlOut:
        self._account(account_id)
        books = self.app.books.get(account_id)
        if books is None:
            raise NotFound(f"no books for account {account_id!r}")
        p = books.portfolio
        now = self.app.clock.now()
        d = day or p.ledger_day(now)
        total = p.ledger.realized(day=d)
        lines = [
            s.PnlLine(
                strategy_id=sid,
                symbol=sym,
                gross=v.gross,
                commission=v.commission,
                swap=v.swap,
                net=v.net,
                deals=v.deals,
            )
            for (sid, sym), v in sorted(p.ledger.attribution(d).items())
        ]
        point = p.book.sample(now) if day is None or d == p.ledger_day(now) else None
        return s.PnlOut(
            account_id=account_id,
            day=d.isoformat(),
            gross=total.gross,
            commission=total.commission,
            swap=total.swap,
            net=total.net,
            deals=total.deals,
            by_strategy_symbol=lines,
            equity=point.equity if point else None,
            unrealized=point.unrealized if point else None,
            high_water=point.high_water if point else None,
            drawdown=point.drawdown if point else None,
        )

    def _trades(
        self,
        account_id: str | None,
        strategy_id: str | None,
        symbol: str | None,
        since: datetime | None,
    ) -> list[TradeRecord]:
        out: list[TradeRecord] = []
        for a in self._accounts(account_id):
            books = self.app.books.get(a.account.id)
            if books is None:
                continue
            out.extend(
                t
                for t in books.journal.trades()
                if (strategy_id is None or t.strategy_id == strategy_id)
                and (symbol is None or t.symbol == symbol)
                and (since is None or t.exit_time >= since)
            )
        out.sort(key=lambda t: t.exit_time, reverse=True)
        return out

    def trades(
        self,
        account_id: str | None = None,
        *,
        strategy_id: str | None = None,
        symbol: str | None = None,
        since: datetime | None = None,
        limit: int = 200,
    ) -> list[s.TradeOut]:
        return [
            s.TradeOut(
                account_id=t.account_id,
                trade_id=t.trade_id,
                strategy_id=t.strategy_id,
                strategy_version=t.strategy_version,
                symbol=t.symbol,
                side=t.side,
                volume=t.volume,
                entry_price=t.entry_price,
                exit_price=t.exit_price,
                entry_time=t.entry_time,
                exit_time=t.exit_time,
                gross=t.gross,
                commission=t.commission,
                swap=t.swap,
                net=t.net,
                r_multiple=t.r_multiple,
                mae=t.mae,
                mfe=t.mfe,
                notes=list(t.notes),
            )
            for t in self._trades(account_id, strategy_id, symbol, since)[:limit]
        ]

    EXECUTION_KEYS = ("broker", "symbol", "strategy", "order_type", "hour", "account")

    def execution(self, account_id: str | None = None, by: tuple[str, ...] = ("symbol",)) -> list[s.StatsOut]:
        unknown = [k for k in by if k not in self.EXECUTION_KEYS]
        if unknown:
            raise ValueError(f"unknown grouping {unknown}; use {', '.join(self.EXECUTION_KEYS)}")
        getters: dict[str, Callable[[ExecutionRecord], object]] = {
            "broker": lambda r: r.broker,
            "symbol": lambda r: r.symbol,
            "strategy": lambda r: r.strategy_id,
            "order_type": lambda r: r.order_type,
            "hour": lambda r: r.hour,
            "account": lambda r: r.account_id,
        }
        records = [
            r
            for a in self._accounts(account_id)
            if (books := self.app.books.get(a.account.id)) is not None
            for r in books.executions.records
        ]
        grouped = execution.stats(records, key=lambda r: tuple(getters[k](r) for k in by))
        return [
            s.StatsOut(key=dict(zip(by, key, strict=True)), stats=dict(st.to_json()))
            for key, st in sorted(grouped.items(), key=lambda kv: str(kv[0]))
        ]

    PERFORMANCE_KEYS = ("strategy", "symbol", "account")

    def performance(
        self,
        account_id: str | None = None,
        by: tuple[str, ...] = ("strategy",),
        since: datetime | None = None,
    ) -> list[s.StatsOut]:
        unknown = [k for k in by if k not in self.PERFORMANCE_KEYS]
        if unknown:
            raise ValueError(f"unknown grouping {unknown}; use {', '.join(self.PERFORMANCE_KEYS)}")
        getters: dict[str, Callable[[TradeRecord], str]] = {
            "strategy": lambda t: t.strategy_id,
            "symbol": lambda t: t.symbol,
            "account": lambda t: t.account_id,
        }
        trades = self._trades(account_id, None, None, since)
        grouped = metrics.group(trades, lambda t: tuple(getters[k](t) for k in by))
        return [
            s.StatsOut(key=dict(zip(by, key, strict=True)), stats=dict(m.to_json()))
            for key, m in sorted(grouped.items())
        ]

    async def audit(
        self, *, limit: int = 100, action: str | None = None, account_id: str | None = None
    ) -> list[s.AuditOut]:
        entries = await self.app.audit_log.recent(limit=limit, action=action, account_id=account_id)
        return [_audit_out(e) for e in entries]

    # ================================================================== mutate

    async def submit_order(self, body: s.ManualOrderIn) -> s.OrderOut:
        services = self._account(body.account)
        details = body.model_dump(mode="json")

        async def run() -> Order:
            intent = OrderIntent(
                symbol=body.symbol,
                side=body.side,
                order_type=body.order_type,
                volume=body.volume,
                strategy_id=MANUAL,
                strategy_version=self.actor,
                signal_ts=self.app.clock.now(),
                limit_price=body.limit_price,
                stop_price=body.stop_price,
                sl=body.sl,
                tp=body.tp,
                time_in_force=body.time_in_force,
                expiry=body.expiry,
                max_slippage=body.max_slippage,
                tags=(f"by:{self.actor}",),
            )
            return await services.oms.submit(intent)

        def describe(o: Order) -> str:
            reason = f" ({o.rejection_reason})" if o.rejection_reason else ""
            return f"{o.status} {o.request.client_order_id}{reason}"

        order = await self._audited("order_submit", body.account, details, run, describe)
        return self._order_out(body.account, order)

    async def cancel_order(self, client_order_id: str, reason: str) -> s.CommandOut:
        for aid, a in self.app.accounts.items():
            if a.oms.order(client_order_id) is not None:

                async def run(a: AccountServices = a) -> None:
                    await a.oms.cancel_order(client_order_id, reason)

                await self._audited(
                    "order_cancel", aid, {"client_order_id": client_order_id, "reason": reason}, run
                )
                return s.CommandOut(ok=True, message=f"cancel requested for {client_order_id}")
        raise NotFound(f"unknown order {client_order_id!r}")

    async def close_position(self, position_id: str, body: s.ClosePositionIn) -> s.CommandOut:
        services = self._account(body.account)
        if not any(p.position_id == position_id for p in services.oms.positions()):
            raise NotFound(f"unknown position {position_id!r} on {body.account}")

        async def run() -> None:
            await services.oms.close_position(position_id, body.volume)

        details = {"position_id": position_id, "volume": body.volume, "reason": body.reason}
        await self._audited("position_close", body.account, details, run)
        what = "close" if body.volume is None else f"close {body.volume} lots"
        return s.CommandOut(ok=True, message=f"{what} requested for {position_id}")

    async def kill(self, account_id: str, level: KillLevel, reason: str) -> s.RiskAccountOut:
        services = self._account(account_id)
        if level is KillLevel.NONE:
            raise ValueError("use the reset endpoint to clear the kill switch")
        await self._audited(
            "kill",
            account_id,
            {"level": str(level), "reason": reason},
            lambda: services.oms.kill_switch(level, reason),
            lambda new: f"level={new}",
        )
        return self.risk(account_id)[0]

    async def reset(self, account_id: str, reason: str) -> s.RiskAccountOut:
        services = self._account(account_id)
        await self._audited(
            "kill_reset",
            account_id,
            {"reason": reason},
            lambda: services.oms.reset_kill_switch(reason),
            lambda new: f"level={new}",
        )
        return self.risk(account_id)[0]

    async def kill_all(self, level: KillLevel, reason: str) -> list[s.RiskAccountOut]:
        for aid in list(self.app.accounts):
            await self.kill(aid, level, reason)
        return self.risk()

    def _runner_strategy(self, strategy_id: str) -> None:
        runner = self.app.runner
        if runner is None or strategy_id not in runner.handles:
            raise NotFound(f"unknown strategy {strategy_id!r}")

    async def create_strategy(self, body: s.StrategyCreateIn) -> s.StrategyOut:
        """Only built-in strategies, or classes the config file already uses: an API call never makes the
        process import a module it was not configured with."""
        allowed = set(BUILTIN) | {st.class_ for st in self.app.config.strategies}
        if body.class_ not in allowed:
            raise ValueError(f"class {body.class_!r}: via the API use one of {sorted(allowed)}")
        cfg = StrategyConfig.model_validate(body.model_dump(by_alias=True, exclude={"start"}))

        async def run() -> None:
            await self.app.add_strategy(cfg, start=body.start)

        await self._audited("strategy_create", cfg.account, body.model_dump(mode="json", by_alias=True), run)
        return self.strategy(cfg.id)

    async def _strategy_command(
        self, action: str, strategy_id: str, details: Mapping[str, object], run: Callable[[], Awaitable[Any]]
    ) -> s.StrategyOut:
        self._runner_strategy(strategy_id)
        assert self.app.runner is not None
        account = self.app.runner.handles[strategy_id].spec.account
        try:
            await self._audited(action, account, {"strategy_id": strategy_id, **details}, run)
        except ValueError as exc:  # the runner refuses a transition (e.g. resume under a kill switch)
            raise Conflict(str(exc)) from None
        return self.strategy(strategy_id)

    async def start_strategy(self, strategy_id: str) -> s.StrategyOut:
        """Start a LOADED or STOPPED strategy, or resume a PAUSED one."""

        async def run() -> None:
            runner = self.app.runner
            assert runner is not None
            status = runner.handles[strategy_id].status
            if status is StrategyStatus.PAUSED:
                runner.resume(strategy_id)
            elif status is StrategyStatus.STOPPED:
                await self.app.restart_strategy(strategy_id)
            elif status is StrategyStatus.LOADED:
                await runner.start(strategy_id)
            else:
                raise ValueError(f"{strategy_id} is already {status}")

        return await self._strategy_command("strategy_start", strategy_id, {}, run)

    async def pause_strategy(self, strategy_id: str, reason: str) -> s.StrategyOut:
        async def run() -> None:
            runner = self.app.runner
            assert runner is not None
            if runner.handles[strategy_id].status is not StrategyStatus.RUNNING:
                raise ValueError(f"{strategy_id} is {runner.handles[strategy_id].status}, not RUNNING")
            runner.pause(strategy_id, reason)

        return await self._strategy_command("strategy_pause", strategy_id, {"reason": reason}, run)

    async def stop_strategy(self, strategy_id: str) -> s.StrategyOut:
        async def run() -> None:
            assert self.app.runner is not None
            await self.app.runner.stop(strategy_id)

        return await self._strategy_command("strategy_stop", strategy_id, {}, run)

    async def set_params(self, strategy_id: str, params: Mapping[str, Any]) -> s.StrategyOut:
        """New params = a new instance (state carries over); a paused strategy stays paused."""
        self._runner_strategy(strategy_id)
        assert self.app.runner is not None
        cls = self.app.runner.handles[strategy_id].spec.cls
        cls.Params.model_validate(dict(params))  # invalid params: 422, before anything stops

        async def run() -> None:
            await self.app.replace_strategy_params(strategy_id, dict(params))

        return await self._strategy_command("strategy_params", strategy_id, {"params": dict(params)}, run)

    async def add_note(self, trade_id: str, account_id: str, text: str) -> s.CommandOut:
        books = self.app.books.get(account_id)
        if books is None or not books.journal.known(trade_id):
            raise NotFound(f"unknown trade {trade_id!r} on {account_id}")

        async def run() -> None:
            books.journal.add_note(trade_id, text)
            await books.portfolio.flush()

        await self._audited("trade_note", account_id, {"trade_id": trade_id, "text": text}, run)
        return s.CommandOut(ok=True, message="note added")


def _audit_out(e: AuditEntry) -> s.AuditOut:
    return s.AuditOut(
        ts=e.ts, actor=e.actor, action=e.action, account_id=e.account_id, details=e.details, result=e.result
    )
