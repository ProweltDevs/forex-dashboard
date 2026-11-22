"""Event store and projections on a real PostgreSQL: schema migrated on the first write, every domain
record projected, replays idempotent, and the writer surviving a hard database kill."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from tests.factories import intent
from tests.pg import TempPostgres
from trader.domain import (
    AccountEvent,
    FillEvent,
    OrderEvent,
    OrderStatus,
    Position,
    PositionChange,
    PositionEvent,
    ReconcileEvent,
    RiskDecision,
    RiskEvent,
    Side,
)
from trader.domain.models import AccountState, Fill, Order
from trader.domain.time import SystemClock
from trader.oms.state_machine import apply_fill, transition
from trader.persistence.serialize import Record, event_record, record
from trader.persistence.sink import PostgresSink
from trader.persistence.writer import DbWriter

D = Decimal
T0 = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)


def lifecycle() -> list[Record]:
    """An order from NEW to FILLED, its fill, the position opened and closed, plus the other record kinds."""
    req = intent(sl=D("1.09"), signal_ts=T0, strategy_id="ema").to_request("01J00000000000000000000000")
    order = Order.new(req, T0)
    out: list[Record] = [
        event_record(OrderEvent(account_id="a", order=order, previous_status=None, ts=T0), "a", T0)
    ]
    for i, status in enumerate((OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED), start=1):
        order = transition(order, status, T0 + timedelta(seconds=i))
        out.append(
            event_record(OrderEvent(account_id="a", order=order, previous_status=None, ts=T0), "a", T0)
        )
    order = apply_fill(order, D("0.10"), D("1.10002"), T0 + timedelta(seconds=3)).with_changes(
        broker_order_id="B1"
    )
    out.append(event_record(OrderEvent(account_id="a", order=order, previous_status=None, ts=T0), "a", T0))
    fill = Fill(
        fill_id="F1",
        client_order_id=req.client_order_id,
        broker_order_id="B1",
        position_id="P1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.10"),
        price=D("1.10002"),
        ts=T0,
        commission=D("-0.3"),
    )
    out.append(event_record(FillEvent(account_id="a", fill=fill, ts=T0), "a", T0))
    pos = Position(
        position_id="P1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.10"),
        entry_price=D("1.10002"),
        opened_ts=T0,
        strategy_id="ema",
        sl=D("1.09"),
    )
    for change in (PositionChange.OPENED, PositionChange.CLOSED):
        out.append(event_record(PositionEvent(account_id="a", position=pos, change=change, ts=T0), "a", T0))
    acct = AccountState(
        account_id="a",
        currency="USD",
        balance=D(10_000),
        equity=D("10001.5"),
        margin=D(110),
        free_margin=D("9891.5"),
        leverage=100,
        ts=T0,
    )
    out.append(event_record(AccountEvent(account_id="a", state=acct, ts=T0), "a", T0))
    out.append(
        event_record(
            RiskEvent(
                account_id="a",
                decision=RiskDecision.APPROVED,
                rule="all_rules",
                reason="ok",
                ts=T0,
                strategy_id="ema",
                client_order_id=req.client_order_id,
            ),
            "a",
            T0,
        )
    )
    out.append(
        event_record(
            ReconcileEvent(
                account_id="a",
                kind="position_adopted",
                entity_id="P9",
                before=None,
                after="x",
                reason="startup",
                ts=T0,
            ),
            "a",
            T0,
        )
    )
    out.append(
        record(
            "audit",
            "a",
            T0,
            {"actor": "cli", "action": "kill", "details": {"level": "PAUSE_NEW"}, "result": "ok"},
        )
    )
    out.append(
        record(
            "strategy",
            "a",
            T0,
            {
                "strategy_id": "ema",
                "class_name": "EmaCross",
                "version": "1",
                "params": {"fast": 12},
                "params_hash": "h",
                "status": "RUNNING",
            },
        )
    )
    return out


async def rows(url: str, query: str) -> list[tuple[object, ...]]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(r) for r in (await conn.execute(sa.text(query))).all()]
    finally:
        await engine.dispose()


async def test_schema_is_migrated_and_every_record_is_projected_idempotently(pg_url: str) -> None:
    sink = PostgresSink(pg_url)
    records = lifecycle()
    await sink.write(records)
    await sink.write(records)  # a replay: nothing changes
    await sink.close()
    assert await rows(pg_url, "select version_num from alembic_version") == [("0001",)]
    assert await rows(pg_url, "select count(*) from events") == [(len(records),)]
    assert await rows(pg_url, "select status, filled_volume, broker_order_id, strategy_id from orders") == [
        ("FILLED", D("0.10"), "B1", "ema")
    ]
    assert await rows(pg_url, "select seq, status from order_events order by seq") == [
        (0, "NEW"),
        (1, "PENDING_SUBMIT"),
        (2, "ACCEPTED"),
        (3, "FILLED"),
    ]
    assert await rows(pg_url, "select fill_id, price, commission from fills") == [
        ("F1", D("1.10002"), D("-0.3"))
    ]
    assert await rows(pg_url, "select open from positions") == [(False,)]
    assert await rows(pg_url, "select change from position_history order by id") == [("OPENED",), ("CLOSED",)]
    assert await rows(pg_url, "select balance, equity from account_snapshots") == [(D(10_000), D("10001.5"))]
    assert await rows(pg_url, "select decision, rule from risk_events") == [("APPROVED", "all_rules")]
    assert await rows(pg_url, "select kind from reconcile_events") == [("position_adopted",)]
    assert await rows(pg_url, "select actor, action, result from audit_log") == [("cli", "kill", "ok")]
    assert await rows(pg_url, "select status, params from strategies") == [("RUNNING", {"fast": 12})]


async def test_writer_survives_a_hard_database_kill(pg_own: TempPostgres, tmp_path: Path) -> None:
    w = DbWriter(
        PostgresSink(pg_own.url),
        tmp_path / "wal" / "db.wal",
        SystemClock(),
        flush_interval=timedelta(milliseconds=20),
        retry_interval=timedelta(milliseconds=200),
    )
    await w.start()

    def rec(n: int) -> Record:
        return record("test", "a", T0, {"n": n})

    for n in range(50):
        w.submit(rec(n))
    async with asyncio.timeout(15):
        while w.stats.written < 50:
            await asyncio.sleep(0.05)
    pg_own.kill()  # the outage
    for n in range(50, 150):
        w.submit(rec(n))  # never blocks
        await asyncio.sleep(0.002)
    async with asyncio.timeout(15):
        while not (w.down and w.wal.count() >= 100):
            await asyncio.sleep(0.05)
    pg_own.start()
    async with asyncio.timeout(30):
        while w.down or w.pending:
            await asyncio.sleep(0.1)
    await w.stop()
    got = await rows(pg_own.url, "select (payload->>'n')::int from events order by id")
    assert [g[0] for g in got] == list(range(150))  # all of them, once each, in order
    assert w.stats.outages == 1
    assert not w.wal.path.exists()
