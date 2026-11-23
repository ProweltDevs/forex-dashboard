"""The migrations create exactly the schema ``persistence/schema.py`` describes: no drift. A model change
without a new Alembic revision fails here (``alembic revision --autogenerate`` writes the missing one)."""

from __future__ import annotations

import asyncio

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from trader.persistence import migrate
from trader.persistence.schema import metadata


async def test_migrations_match_the_models(pg_url: str) -> None:
    await asyncio.to_thread(migrate.upgrade, pg_url)

    def diff(conn: Connection) -> list[object]:
        return list(compare_metadata(MigrationContext.configure(conn), metadata))

    engine = create_async_engine(pg_url)
    try:
        async with engine.connect() as conn:
            changes = await conn.run_sync(diff)
            version = (await conn.exec_driver_sql("select version_num from alembic_version")).scalar_one()
    finally:
        await engine.dispose()
    assert changes == [], f"schema.py and the migrations differ: {changes}"
    assert version == migrate.head()
