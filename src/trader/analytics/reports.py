"""Daily report (SPEC §10): P&L, trades, risk decisions and execution stats for one account and trading
day, stored as JSON plus CSV exports of that day's trades and executions
(``{data_dir}/reports/{account}/{day}.json``, ``{day}-trades.csv``, ``{day}-executions.csv``)."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

from trader.domain.jsontypes import JsonObj
from trader.persistence.files import write_atomic
from trader.portfolio.book import EquityPoint
from trader.portfolio.ledger import FillLedger, trading_day

from .execution import ExecutionRecord, stats
from .journal import TradeRecord
from .metrics import compute, group
from .types import Out, OutObj


def daily_report(
    *,
    account_id: str,
    day: date,
    ledger: FillLedger,
    trades: Iterable[TradeRecord],
    executions: Iterable[ExecutionRecord],
    equity: Sequence[EquityPoint] = (),
    risk_counts: Mapping[str, int] | None = None,
    start_equity: Decimal | None = None,
) -> OutObj:
    offset = ledger.day_offset
    day_trades = [t for t in trades if trading_day(t.exit_time, offset) == day]
    day_exec = [e for e in executions if (ts := e.send_ts or e.signal_ts) and trading_day(ts, offset) == day]
    curve = [p for p in equity if trading_day(p.ts, offset) == day]
    by_key: list[Out] = [
        {"strategy_id": sid, "symbol": sym, **_out(pnl.to_json())}
        for (sid, sym), pnl in sorted(ledger.attribution(day).items())
    ]
    pnl: OutObj = {"realized": _out(ledger.realized(day=day).to_json()), "by_strategy_symbol": by_key}
    if curve:
        last = curve[-1]
        pnl |= {
            "equity_close": str(last.equity),
            "unrealized_close": str(last.unrealized),
            "high_water": str(max(p.high_water for p in curve)),
            "max_drawdown": str(max(p.drawdown for p in curve)),
            "broker_equity_close": None if last.broker_equity is None else str(last.broker_equity),
        }
    execution: dict[str, Out] = {
        "overall": next(iter(stats(day_exec, key=lambda _: ("all",)).values())).to_json()
        if day_exec
        else None,
        "by_broker_symbol": [
            {"broker": str(k[0]), "symbol": str(k[1]), **s.to_json()}
            for k, s in sorted(
                stats(day_exec, key=lambda r: (r.broker, r.symbol)).items(), key=lambda kv: str(kv[0])
            )
        ],
    }
    strategies: dict[str, Out] = {
        sid: m.to_json()
        for sid, m in sorted(group(day_trades, lambda t: t.strategy_id, start_equity=start_equity).items())
    }
    return {
        "account_id": account_id,
        "day": day.isoformat(),
        "pnl": pnl,
        "trades": compute(day_trades, start_equity=start_equity).to_json(),
        "by_strategy": strategies,
        "risk": dict(sorted((risk_counts or {}).items())),
        "execution": execution,
    }


def _out(data: JsonObj) -> OutObj:
    """Domain JSON values are a subset of analytics output values (dicts are invariant, hence the cast)."""
    return cast("OutObj", data)


def _csv(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v) if isinstance(v, list | dict) else v for k, v in row.items()})
    return buf.getvalue()


def write_report(
    directory: Path,
    report: OutObj,
    trades: Sequence[TradeRecord],
    executions: Sequence[ExecutionRecord],
) -> list[Path]:
    """Write the report JSON and the day's CSV exports (blocking; call off the event loop)."""
    day = str(report["day"])
    paths = [directory / f"{day}.json"]
    write_atomic(paths[0], json.dumps(report, indent=2))
    for name, rows in (
        ("trades", [t.to_json() for t in trades]),
        ("executions", [e.to_json() for e in executions]),
    ):
        if rows:
            path = directory / f"{day}-{name}.csv"
            write_atomic(path, _csv(rows))
            paths.append(path)
    return paths
