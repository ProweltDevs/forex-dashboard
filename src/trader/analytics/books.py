"""Per-account books: the PortfolioService with its trade journal and execution tracker, built the same way
by ``trader run`` (live) and loaded from files by ``trader report`` (offline).

Files under ``data_dir``: ``portfolio/{account}/fills.jsonl`` and ``equity-{day}.jsonl``,
``journal/{account}/trades.jsonl`` and ``notes.jsonl``, ``executions/{account}.jsonl``,
``reports/{account}/{day}.json`` (+ CSVs).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from trader.config.settings import AccountConfig, AppConfig
from trader.domain.time import Clock
from trader.marketdata.bus import TickBus
from trader.oms import OrderManager
from trader.portfolio.book import EquityPoint
from trader.portfolio.ledger import FillLedger, trading_day
from trader.portfolio.service import DealSource, PortfolioService, PortfolioSettings

from .execution import ExecutionRecord, ExecutionTracker
from .journal import TradeJournal, TradeRecord
from .reports import daily_report, write_report
from .types import OutObj


def params_hash(params: Mapping[str, object]) -> str:
    """Short stable hash of a strategy's params (stored with every trade)."""
    return hashlib.sha256(json.dumps(params, sort_keys=True, default=str).encode()).hexdigest()[:12]


@dataclass(slots=True)
class AccountBooks:
    account: AccountConfig
    portfolio: PortfolioService
    journal: TradeJournal
    executions: ExecutionTracker

    def report(self, day: date) -> OutObj:
        p = self.portfolio
        first = next((pt for pt in p.curve if p.ledger_day(pt.ts) == day), None)
        return daily_report(
            account_id=self.account.id,
            day=day,
            ledger=p.ledger,
            trades=self.journal.trades(),
            executions=self.executions.records,
            equity=p.curve,
            risk_counts=p.risk_counts.get(day, {}),
            start_equity=None if first is None else first.equity,
        )

    async def write_report(self, data_dir: Path, day: date) -> list[Path]:
        report = self.report(day)
        trades = [t for t in self.journal.trades() if self.portfolio.ledger_day(t.exit_time) == day]
        execs = [
            e for e in self.executions.records if (ts := e.send_ts) and self.portfolio.ledger_day(ts) == day
        ]
        return await asyncio.to_thread(
            write_report, data_dir / "reports" / self.account.id, report, trades, execs
        )


def build_books(
    config: AppConfig,
    account: AccountConfig,
    oms: OrderManager,
    broker: DealSource,
    broker_name: str,
    clock: Clock,
    *,
    ticks: TickBus | None = None,
) -> AccountBooks:
    data_dir = config.data_dir
    hashes = {s.id: params_hash(s.params) for s in config.strategies if s.account == account.id}

    def currency() -> str | None:
        acct = oms.view().account
        return None if acct is None else acct.currency

    journal = TradeJournal(
        account.id,
        currency=currency,
        instruments=lambda: oms.view().instruments,
        order_lookup=oms.order,
        params_hash=hashes,
        directory=data_dir / "journal" / account.id,
    )
    executions = ExecutionTracker(
        account.id,
        broker_name,
        currency=currency,
        instruments=lambda: oms.view().instruments,
        path=data_dir / "executions" / f"{account.id}.jsonl",
    )
    p = config.portfolio
    portfolio = PortfolioService(
        account.id,
        oms,
        broker,
        clock,
        ticks=ticks,
        data_dir=data_dir,
        day_offset_minutes=account.risk.day_offset_minutes,
        settings=PortfolioSettings(
            equity_sample=p.equity_sample,
            drift_pct=p.drift_pct,
            drift_min=p.drift_min,
            balance_grace=p.balance_grace,
        ),
        listeners=[journal, executions],
    )
    return AccountBooks(account=account, portfolio=portfolio, journal=journal, executions=executions)


# ------------------------------------------------------------------------------------------------ offline


@dataclass(slots=True)
class OfflineBooks:
    ledger: FillLedger
    trades: list[TradeRecord]
    executions: list[ExecutionRecord]
    equity: list[EquityPoint]


def load_offline(data_dir: Path, account: AccountConfig, day: date | None = None) -> OfflineBooks:
    """Read an account's books from disk (blocking)."""
    ledger = FillLedger(data_dir / "portfolio" / account.id / "fills.jsonl", account.risk.day_offset_minutes)
    ledger.load()
    journal = TradeJournal(
        account.id,
        currency=lambda: None,
        instruments=dict,
        order_lookup=lambda _cid: None,
        directory=data_dir / "journal" / account.id,
    )
    journal.load()
    executions: list[ExecutionRecord] = []
    ex_path = data_dir / "executions" / f"{account.id}.jsonl"
    if ex_path.exists():
        executions = [
            ExecutionRecord.from_json(json.loads(x)) for x in ex_path.read_text().splitlines() if x.strip()
        ]
    equity: list[EquityPoint] = []
    pdir = data_dir / "portfolio" / account.id
    for path in sorted(pdir.glob("equity-*.jsonl")) if pdir.exists() else []:
        if day is None or path.stem == f"equity-{day.isoformat()}":
            equity += [
                EquityPoint.from_json(json.loads(x)) for x in path.read_text().splitlines() if x.strip()
            ]
    return OfflineBooks(ledger=ledger, trades=journal.trades(), executions=executions, equity=equity)


def offline_report(data_dir: Path, account: AccountConfig, day: date) -> tuple[OutObj, list[Path]]:
    books = load_offline(data_dir, account, day)
    start_equity: Decimal | None = books.equity[0].equity if books.equity else None
    report = daily_report(
        account_id=account.id,
        day=day,
        ledger=books.ledger,
        trades=books.trades,
        executions=books.executions,
        equity=books.equity,
        start_equity=start_equity,
    )
    offset = account.risk.day_offset_minutes
    trades = [t for t in books.trades if trading_day(t.exit_time, offset) == day]
    execs = [e for e in books.executions if (ts := e.send_ts) and trading_day(ts, offset) == day]
    return report, write_report(data_dir / "reports" / account.id, report, trades, execs)
