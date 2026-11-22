"""Database sink: each record becomes one ``events`` row plus its projection rows, all idempotent (insert on
conflict do nothing, or upsert by natural key), so the WAL replay can repeat records safely. One batch =
one transaction: it commits entirely or not at all."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from sqlalchemy import Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.sql import Executable

from trader.domain.jsontypes import JsonObj, JsonValue
from trader.domain.time import from_epoch_ms
from trader.observability.logging import get_logger

from . import schema as t
from .migrate import upgrade
from .serialize import Record

_log = get_logger("trader.persistence.sink")


class Sink(Protocol):
    async def write(self, records: Sequence[Record]) -> None:
        """Write a batch atomically; raise on any failure (the caller buffers it)."""
        ...

    async def close(self) -> None: ...


# ------------------------------------------------------------------------------------------- projections


def _d(v: JsonValue) -> Decimal | None:
    return None if v is None else Decimal(str(v))


def _ts(v: JsonValue) -> datetime | None:
    return None if v is None else from_epoch_ms(int(str(v)))


def _obj(v: JsonValue) -> JsonObj:
    assert isinstance(v, dict)
    return v


def _s(v: JsonValue) -> str | None:
    return None if v is None else str(v)


def _upsert(table: Table, values: dict[str, object], keys: Sequence[str]) -> Executable:
    stmt = insert(table).values(**values)
    return stmt.on_conflict_do_update(
        index_elements=list(keys), set_={k: v for k, v in values.items() if k not in keys}
    )


def _append(table: Table, values: dict[str, object]) -> Executable:
    return insert(table).values(**values).on_conflict_do_nothing()


def statements(r: Record) -> list[Executable]:
    """The event-store row and the projection rows for one record."""
    out: list[Executable] = [
        _append(
            t.events,
            {
                "record_id": r.record_id,
                "kind": r.kind,
                "account_id": r.account_id,
                "ts": r.ts,
                "payload": r.payload,
            },
        )
    ]
    p, acct = r.payload, r.account_id or ""
    match r.kind:
        case "OrderEvent":
            o = _obj(p["order"])
            req = _obj(o["request"])
            history = o["history"]
            assert isinstance(history, list)
            first, last = _obj(history[0]), _obj(history[-1])
            out.append(
                _upsert(
                    t.orders,
                    {
                        "client_order_id": req["client_order_id"],
                        "account_id": acct,
                        "broker_order_id": o.get("broker_order_id"),
                        "strategy_id": req["strategy_id"],
                        "strategy_version": req["strategy_version"],
                        "symbol": req["symbol"],
                        "side": req["side"],
                        "order_type": req["order_type"],
                        "volume": _d(req["volume"]),
                        "limit_price": _d(req.get("limit_price")),
                        "stop_price": _d(req.get("stop_price")),
                        "sl": _d(req.get("sl")),
                        "tp": _d(req.get("tp")),
                        "status": o["status"],
                        "filled_volume": _d(o["filled_volume"]),
                        "avg_fill_price": _d(o.get("avg_fill_price")),
                        "rejection_reason": o.get("rejection_reason"),
                        "signal_ts": _ts(req.get("signal_ts")),
                        "created_at": _ts(first["ts"]),
                        "updated_at": _ts(last["ts"]),
                        "request": req,
                    },
                    ["client_order_id"],
                )
            )
            for seq, h in enumerate(history):
                h = _obj(h)
                out.append(
                    _append(
                        t.order_events,
                        {
                            "client_order_id": req["client_order_id"],
                            "seq": seq,
                            "account_id": acct,
                            "status": h["status"],
                            "ts": _ts(h["ts"]),
                            "reason": h.get("reason"),
                        },
                    )
                )
        case "FillEvent":
            f = _obj(p["fill"])
            out.append(
                _append(
                    t.fills,
                    {
                        "account_id": acct,
                        "fill_id": f["fill_id"],
                        "client_order_id": f.get("client_order_id"),
                        "broker_order_id": f["broker_order_id"],
                        "position_id": f["position_id"],
                        "symbol": f["symbol"],
                        "side": f["side"],
                        "volume": _d(f["volume"]),
                        "price": _d(f["price"]),
                        "ts": _ts(f["ts"]),
                        "commission": _d(f["commission"]),
                        "swap": _d(f["swap"]),
                        "realized_pnl": _d(f.get("realized_pnl")),
                        "balance_after": _d(f.get("balance_after")),
                    },
                )
            )
        case "PositionEvent":
            pos, change = _obj(p["position"]), str(p["change"])
            out.append(
                _upsert(
                    t.positions,
                    {
                        "account_id": acct,
                        "position_id": pos["position_id"],
                        "symbol": pos["symbol"],
                        "side": pos["side"],
                        "volume": _d(pos["volume"]),
                        "entry_price": _d(pos["entry_price"]),
                        "sl": _d(pos.get("sl")),
                        "tp": _d(pos.get("tp")),
                        "strategy_id": pos["strategy_id"],
                        "opened_ts": _ts(pos["opened_ts"]),
                        "open": change != "CLOSED",
                        "updated_at": r.ts,
                    },
                    ["account_id", "position_id"],
                )
            )
            out.append(
                _append(
                    t.position_history,
                    {
                        "record_id": r.record_id,
                        "account_id": acct,
                        "position_id": pos["position_id"],
                        "change": change,
                        "volume": _d(pos["volume"]),
                        "sl": _d(pos.get("sl")),
                        "tp": _d(pos.get("tp")),
                        "ts": r.ts,
                    },
                )
            )
        case "AccountEvent":
            s = _obj(p["state"])
            out.append(
                _append(
                    t.account_snapshots,
                    {
                        "record_id": r.record_id,
                        "account_id": acct,
                        "ts": _ts(s["ts"]),
                        "currency": s["currency"],
                        "balance": _d(s["balance"]),
                        "equity": _d(s["equity"]),
                        "margin": _d(s["margin"]),
                        "free_margin": _d(s["free_margin"]),
                        "margin_level": _d(s.get("margin_level")),
                    },
                )
            )
        case "RiskEvent":
            out.append(
                _append(
                    t.risk_events,
                    {
                        "record_id": r.record_id,
                        "account_id": acct,
                        "ts": r.ts,
                        "decision": p["decision"],
                        "rule": p["rule"],
                        "reason": p["reason"],
                        "strategy_id": p.get("strategy_id"),
                        "client_order_id": p.get("client_order_id"),
                    },
                )
            )
        case "ReconcileEvent":
            out.append(
                _append(
                    t.reconcile_events,
                    {
                        "record_id": r.record_id,
                        "account_id": acct,
                        "ts": r.ts,
                        "kind": p["kind"],
                        "entity_id": p["entity_id"],
                        "before": p.get("before"),
                        "after": p.get("after"),
                        "reason": p["reason"],
                    },
                )
            )
        case "trade":
            out.append(
                _upsert(
                    t.trades,
                    {
                        "account_id": acct,
                        "trade_id": p["trade_id"],
                        "strategy_id": p["strategy_id"],
                        "strategy_version": p["strategy_version"],
                        "params_hash": p["params_hash"],
                        "symbol": p["symbol"],
                        "side": p["side"],
                        "volume": _d(p["volume"]),
                        "entry_price": _d(p["entry_price"]),
                        "exit_price": _d(p["exit_price"]),
                        "entry_time": _ts(p["entry_time_ms"]),
                        "exit_time": _ts(p["exit_time_ms"]),
                        "gross": _d(p["gross"]),
                        "commission": _d(p["commission"]),
                        "swap": _d(p["swap"]),
                        "net": _d(p["net"]),
                        "r_multiple": _d(p.get("r_multiple")),
                        "payload": p,
                    },
                    ["account_id", "trade_id"],
                )
            )
        case "strategy":
            out.append(
                _upsert(
                    t.strategies,
                    {
                        "strategy_id": p["strategy_id"],
                        "account_id": acct,
                        "class_name": p["class_name"],
                        "version": p["version"],
                        "params": p["params"],
                        "params_hash": p["params_hash"],
                        "status": p["status"],
                        "updated_at": r.ts,
                    },
                    ["strategy_id"],
                )
            )
        case "strategy_state":
            out.append(
                _upsert(
                    t.strategy_state,
                    {"strategy_id": p["strategy_id"], "state": p["state"], "updated_at": r.ts},
                    ["strategy_id"],
                )
            )
        case "account":
            out.append(
                _upsert(
                    t.accounts,
                    {
                        "account_id": acct,
                        "broker": p["broker"],
                        "environment": p["environment"],
                        "broker_account": p.get("broker_account"),
                        "updated_at": r.ts,
                    },
                    ["account_id"],
                )
            )
        case "instrument":
            out.append(
                _upsert(
                    t.instruments,
                    {
                        "account_id": acct,
                        "symbol": p["symbol"],
                        "broker_symbol": p["broker_symbol"],
                        "digits": p["digits"],
                        "tick_size": _d(p["tick_size"]),
                        "contract_size": _d(p["contract_size"]),
                        "min_volume": _d(p["min_volume"]),
                        "max_volume": _d(p["max_volume"]),
                        "volume_step": _d(p["volume_step"]),
                        "base_currency": p["base_currency"],
                        "quote_currency": p["quote_currency"],
                        "margin_currency": p["margin_currency"],
                        "updated_at": r.ts,
                    },
                    ["account_id", "symbol"],
                )
            )
        case "audit":
            out.append(
                _append(
                    t.audit_log,
                    {
                        "record_id": r.record_id,
                        "ts": r.ts,
                        "actor": p["actor"],
                        "action": p["action"],
                        "account_id": r.account_id,
                        "details": p.get("details") or {},
                        "result": p["result"],
                    },
                )
            )
        case _:
            pass  # event store only (risk state, strategy, retry, alert, connection events)
    return out


# ------------------------------------------------------------------------------------------- postgres


class PostgresSink:
    """Writes batches to PostgreSQL. On the first successful connection it upgrades the schema to head."""

    def __init__(self, url: str, *, pool_size: int = 5, migrate: bool = True) -> None:
        self.url = url
        self._engine: AsyncEngine = create_async_engine(
            url, pool_size=pool_size, pool_pre_ping=True, connect_args={"timeout": 5, "command_timeout": 15}
        )
        self._migrate = migrate
        self._migrated = False

    async def _ensure_schema(self) -> None:
        if self._migrated:
            return
        async with self._engine.connect() as conn:
            await conn.execute(text("select 1"))  # fail fast while the database is down
        if self._migrate:
            await asyncio.to_thread(upgrade, self.url)
        self._migrated = True
        _log.info("db_schema_ready", url=self.redacted)

    async def write(self, records: Sequence[Record]) -> None:
        await self._ensure_schema()
        async with self._engine.begin() as conn:
            for r in records:
                for stmt in statements(r):
                    await conn.execute(stmt)

    async def close(self) -> None:
        await self._engine.dispose()

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def redacted(self) -> str:
        return self._engine.url.render_as_string(hide_password=True)
