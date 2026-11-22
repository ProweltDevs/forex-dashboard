"""PostgreSQL schema (SPEC §12), SQLAlchemy Core. Alembic manages it (``persistence/migrations``).

- ``events``: the append-only event store: every domain event except ticks and bars, plus journal trades,
  strategy state and audit entries, as JSONB. ``record_id`` (ULID) makes replays idempotent.
- Projections for queries, kept current from the same records: ``orders`` (latest), ``order_events``
  (every status change), ``fills``, ``positions`` (+ ``position_history``), ``account_snapshots``,
  ``risk_events``, ``reconcile_events``, ``trades``, ``strategies``, ``strategy_state``, ``accounts``,
  ``instruments``, ``audit_log``.

Money and prices are ``NUMERIC`` (exact, like the domain's Decimal); times are ``TIMESTAMPTZ`` (UTC).
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "pk": "pk_%(table_name)s",
    }
)

ID = 64  # ids, symbols, names
TS = DateTime(timezone=True)


def _id() -> Column[int]:
    return Column("id", BigInteger, Identity(), primary_key=True)


def _record_id() -> Column[str]:
    return Column("record_id", String(26), nullable=False, unique=True)


events = Table(
    "events",
    metadata,
    _id(),
    _record_id(),
    Column("kind", String(ID), nullable=False),
    Column("account_id", String(ID)),
    Column("ts", TS, nullable=False),
    Column("recorded_at", TS, nullable=False, server_default=func.now()),
    Column("payload", JSONB, nullable=False),
    Index(None, "account_id", "ts"),
    Index(None, "kind", "ts"),
)

accounts = Table(
    "accounts",
    metadata,
    Column("account_id", String(ID), primary_key=True),
    Column("broker", String(ID), nullable=False),
    Column("environment", String(16), nullable=False),
    Column("broker_account", String(ID)),
    Column("updated_at", TS, nullable=False),
)

instruments = Table(
    "instruments",
    metadata,
    Column("account_id", String(ID), primary_key=True),
    Column("symbol", String(ID), primary_key=True),
    Column("broker_symbol", String(ID), nullable=False),
    Column("digits", Integer, nullable=False),
    Column("tick_size", Numeric, nullable=False),
    Column("contract_size", Numeric, nullable=False),
    Column("min_volume", Numeric, nullable=False),
    Column("max_volume", Numeric, nullable=False),
    Column("volume_step", Numeric, nullable=False),
    Column("base_currency", String(8), nullable=False),
    Column("quote_currency", String(8), nullable=False),
    Column("margin_currency", String(8), nullable=False),
    Column("updated_at", TS, nullable=False),
)

strategies = Table(
    "strategies",
    metadata,
    Column("strategy_id", String(ID), primary_key=True),
    Column("account_id", String(ID), nullable=False),
    Column("class_name", String(200), nullable=False),
    Column("version", String(ID), nullable=False),
    Column("params", JSONB, nullable=False),
    Column("params_hash", String(ID), nullable=False),
    Column("status", String(16), nullable=False),
    Column("updated_at", TS, nullable=False),
)

strategy_state = Table(
    "strategy_state",
    metadata,
    Column("strategy_id", String(ID), primary_key=True),
    Column("state", JSONB, nullable=False),
    Column("updated_at", TS, nullable=False),
)

orders = Table(
    "orders",
    metadata,
    Column("client_order_id", String(128), primary_key=True),
    Column("account_id", String(ID), nullable=False),
    Column("broker_order_id", String(128)),
    Column("strategy_id", String(ID), nullable=False),
    Column("strategy_version", String(ID), nullable=False),
    Column("symbol", String(ID), nullable=False),
    Column("side", String(8), nullable=False),
    Column("order_type", String(16), nullable=False),
    Column("volume", Numeric, nullable=False),
    Column("limit_price", Numeric),
    Column("stop_price", Numeric),
    Column("sl", Numeric),
    Column("tp", Numeric),
    Column("status", String(24), nullable=False),
    Column("filled_volume", Numeric, nullable=False),
    Column("avg_fill_price", Numeric),
    Column("rejection_reason", Text),
    Column("signal_ts", TS),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("request", JSONB, nullable=False),
    Index(None, "account_id", "updated_at"),
    Index(None, "strategy_id", "created_at"),
)

order_events = Table(
    "order_events",
    metadata,
    Column("client_order_id", String(128), primary_key=True),
    Column("seq", Integer, primary_key=True),  # index in the order's status history
    Column("account_id", String(ID), nullable=False),
    Column("status", String(24), nullable=False),
    Column("ts", TS, nullable=False),
    Column("reason", Text),
)

fills = Table(
    "fills",
    metadata,
    Column("account_id", String(ID), primary_key=True),
    Column("fill_id", String(128), primary_key=True),
    Column("client_order_id", String(128)),
    Column("broker_order_id", String(128), nullable=False),
    Column("position_id", String(128), nullable=False),
    Column("symbol", String(ID), nullable=False),
    Column("side", String(8), nullable=False),
    Column("volume", Numeric, nullable=False),
    Column("price", Numeric, nullable=False),
    Column("ts", TS, nullable=False),
    Column("commission", Numeric, nullable=False),
    Column("swap", Numeric, nullable=False),
    Column("realized_pnl", Numeric),
    Column("balance_after", Numeric),
    Index(None, "account_id", "ts"),
)

positions = Table(
    "positions",
    metadata,
    Column("account_id", String(ID), primary_key=True),
    Column("position_id", String(128), primary_key=True),
    Column("symbol", String(ID), nullable=False),
    Column("side", String(8), nullable=False),
    Column("volume", Numeric, nullable=False),
    Column("entry_price", Numeric, nullable=False),
    Column("sl", Numeric),
    Column("tp", Numeric),
    Column("strategy_id", String(ID), nullable=False),
    Column("opened_ts", TS, nullable=False),
    Column("open", Boolean, nullable=False),
    Column("updated_at", TS, nullable=False),
)

position_history = Table(
    "position_history",
    metadata,
    _id(),
    _record_id(),
    Column("account_id", String(ID), nullable=False),
    Column("position_id", String(128), nullable=False),
    Column("change", String(24), nullable=False),
    Column("volume", Numeric, nullable=False),
    Column("sl", Numeric),
    Column("tp", Numeric),
    Column("ts", TS, nullable=False),
    Index(None, "account_id", "position_id"),
)

account_snapshots = Table(
    "account_snapshots",
    metadata,
    _id(),
    _record_id(),
    Column("account_id", String(ID), nullable=False),
    Column("ts", TS, nullable=False),
    Column("currency", String(8), nullable=False),
    Column("balance", Numeric, nullable=False),
    Column("equity", Numeric, nullable=False),
    Column("margin", Numeric, nullable=False),
    Column("free_margin", Numeric, nullable=False),
    Column("margin_level", Numeric),
    Index(None, "account_id", "ts"),
)

risk_events = Table(
    "risk_events",
    metadata,
    _id(),
    _record_id(),
    Column("account_id", String(ID), nullable=False),
    Column("ts", TS, nullable=False),
    Column("decision", String(16), nullable=False),
    Column("rule", String(ID), nullable=False),
    Column("reason", Text, nullable=False),
    Column("strategy_id", String(ID)),
    Column("client_order_id", String(128)),
    Index(None, "account_id", "ts"),
)

reconcile_events = Table(
    "reconcile_events",
    metadata,
    _id(),
    _record_id(),
    Column("account_id", String(ID), nullable=False),
    Column("ts", TS, nullable=False),
    Column("kind", String(ID), nullable=False),
    Column("entity_id", String(128), nullable=False),
    Column("before", Text),
    Column("after", Text),
    Column("reason", Text, nullable=False),
    Index(None, "account_id", "ts"),
)

trades = Table(
    "trades",
    metadata,
    Column("account_id", String(ID), primary_key=True),
    Column("trade_id", String(128), primary_key=True),
    Column("strategy_id", String(ID), nullable=False),
    Column("strategy_version", String(ID), nullable=False),
    Column("params_hash", String(ID), nullable=False),
    Column("symbol", String(ID), nullable=False),
    Column("side", String(8), nullable=False),
    Column("volume", Numeric, nullable=False),
    Column("entry_price", Numeric, nullable=False),
    Column("exit_price", Numeric, nullable=False),
    Column("entry_time", TS, nullable=False),
    Column("exit_time", TS, nullable=False),
    Column("gross", Numeric, nullable=False),
    Column("commission", Numeric, nullable=False),
    Column("swap", Numeric, nullable=False),
    Column("net", Numeric, nullable=False),
    Column("r_multiple", Numeric),
    Column("payload", JSONB, nullable=False),
    Index(None, "strategy_id", "exit_time"),
)

audit_log = Table(
    "audit_log",
    metadata,
    _id(),
    _record_id(),
    Column("ts", TS, nullable=False),
    Column("actor", String(ID), nullable=False),
    Column("action", String(ID), nullable=False),
    Column("account_id", String(ID)),
    Column("details", JSONB, nullable=False),
    Column("result", Text, nullable=False),
    Index(None, "ts"),
)
