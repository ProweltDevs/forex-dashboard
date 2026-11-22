"""Phase 9 acceptance: DB kill test, trading continues, data replayed.

The real ``App`` runs three strategies on the paper broker (synthetic feed) with PostgreSQL persistence.
Mid-run the database is killed hard (``pg_ctl -m immediate``). Trading must carry on: new orders and fills,
no strategy errors, records buffered to the WAL, a ``db_down`` alert. After the database is restarted the
WAL is replayed, and once the app stops the database must hold exactly what the books hold: every fill,
every order with its final status, every closed trade, each event once.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.test_app_run import synthetic_config
from tests.pg import TempPostgres
from trader.app import App
from trader.config.settings import AppConfig
from trader.domain.time import SystemClock


async def query(url: str, sql: str) -> list[tuple[Any, ...]]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(r) for r in (await conn.execute(sa.text(sql))).all()]
    finally:
        await engine.dispose()


async def until(check: Any, timeout: float, what: str) -> None:
    async with asyncio.timeout(timeout):
        while not (await check() if asyncio.iscoroutinefunction(check) else check()):
            await asyncio.sleep(0.1)
    print(f"[db-kill] {what}")


async def test_db_kill_trading_continues_and_data_is_replayed(pg_own: TempPostgres, tmp_path: Path) -> None:
    raw = synthetic_config(tmp_path).model_dump(mode="json", by_alias=True)
    raw["database"] = {"url_env": "TRADER_DB_URL"}
    config = AppConfig.model_validate(raw)
    app = App(config, env={"TRADER_DB_URL": pg_own.url}, clock=SystemClock())
    await app.start()
    persistence = app.persistence
    assert persistence is not None
    writer = persistence.writer
    books = app.books["paper-main"]
    try:

        async def fills_in_db() -> bool:
            try:
                return bool((await query(pg_own.url, "select count(*) from fills"))[0][0])
            except sa.exc.ProgrammingError:
                return False  # the writer creates the schema on its first connection

        await until(fills_in_db, 30, "fills are reaching the database")

        pg_own.kill()  # --- the outage
        fills_at_kill = len(books.portfolio.ledger)
        orders_at_kill = len(app.accounts["paper-main"].oms.orders())
        await until(lambda: writer.down, 15, "writer noticed the outage")
        await until(
            lambda: len(books.portfolio.ledger) >= fills_at_kill + 2, 30, "trading continues while down"
        )
        assert len(app.accounts["paper-main"].oms.orders()) > orders_at_kill
        assert writer.wal.count() > 0
        assert app.runner is not None
        assert all(stats.total_errors == 0 for _sid, _status, _reason, stats in app.runner.status())

        pg_own.start()  # --- back
        await until(lambda: not writer.down and writer.pending == 0, 60, "WAL replayed")
    finally:
        await app.stop()

    # The database now holds exactly what the books hold.
    ledger_ids = {e.fill.fill_id for e in books.portfolio.ledger.entries()}
    db_fills = {r[0] for r in await query(pg_own.url, "select fill_id from fills")}
    assert db_fills == ledger_ids
    oms_orders = {o.client_order_id: o.status.value for o in app.accounts["paper-main"].oms.orders()}
    db_orders = dict(await query(pg_own.url, "select client_order_id, status from orders"))
    assert db_orders == oms_orders
    db_trades = {r[0] for r in await query(pg_own.url, "select trade_id from trades")}
    assert db_trades == {t.trade_id for t in books.journal.closed}
    [(fill_events,)] = await query(pg_own.url, "select count(*) from events where kind = 'FillEvent'")
    assert fill_events == len(ledger_ids)  # each fill event stored exactly once
    alerts = [
        r[0]
        for r in await query(
            pg_own.url, "select payload->>'kind' from events where kind = 'AlertEvent' order by id"
        )
    ]
    assert alerts[:2] == ["db_down", "db_recovered"]
    assert writer.stats.outages == 1
    assert writer.stats.replayed > 0
    assert not writer.wal.path.exists()
    print(
        f"[db-kill] {writer.stats}; fills={len(ledger_ids)} orders={len(oms_orders)} trades={len(db_trades)}"
    )
