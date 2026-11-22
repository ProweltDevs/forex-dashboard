"""Alembic environment (async, asyncpg). The URL comes from the caller (``trader.persistence.migrate``
passes it as ``config.attributes["url"]``) or from ``TRADER_DB_URL``; it is never stored in alembic.ini."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from trader.persistence.schema import metadata

config = context.config
if config.config_file_name is not None and config.attributes.get("configure_logging", True):
    fileConfig(config.config_file_name)

target_metadata = metadata


def _url() -> str:
    url = config.attributes.get("url") or os.environ.get("TRADER_DB_URL")
    if not url:
        raise RuntimeError("set TRADER_DB_URL (e.g. postgresql+asyncpg://user@host/trader)")
    return str(url)


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
