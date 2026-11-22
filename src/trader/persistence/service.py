"""Persistence: turns the running process's events into records for the DbWriter (ADR 0011).

``attach(bus, account_id)`` persists every domain event published on that bus except ticks and bars (they
live in Parquet). ``put(kind, account_id, payload)`` persists a named record: ``account``, ``instrument``,
``strategy``, ``strategy_state``, ``trade``, ``audit``. Nothing here blocks: ``DbWriter.submit`` only queues.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime

from trader.config.settings import AppConfig
from trader.domain.errors import ConfigError
from trader.domain.events import BarEvent, TickEvent
from trader.domain.time import Clock
from trader.marketdata.bus import Bus, Subscription
from trader.observability.logging import get_logger

from .serialize import event_record, record
from .sink import PostgresSink
from .writer import DbWriter

_log = get_logger("trader.persistence")


class Persistence:
    def __init__(self, writer: DbWriter, clock: Clock) -> None:
        self.writer = writer
        self._clock = clock
        self._tasks: list[asyncio.Task[None]] = []
        self._subs: list[Subscription[object]] = []

    @classmethod
    def from_config(cls, config: AppConfig, env: dict[str, str] | None, clock: Clock) -> Persistence | None:
        db = config.database
        if db is None:
            return None
        url = (env or {}).get(db.url_env)
        if not url:
            raise ConfigError(f"database is configured but {db.url_env} is not set")
        writer = DbWriter(
            PostgresSink(url, pool_size=db.pool_size), config.data_dir / "wal" / "db.wal", clock
        )
        return cls(writer, clock)

    async def start(self) -> None:
        await self.writer.start()
        self.attach(self.writer.events, None)  # db_down / db_recovered go into the store as well

    def attach[E](self, bus: Bus[E], account_id: str | None, name: str = "persistence") -> None:
        sub = bus.subscribe(f"{name}-{account_id or 'app'}")
        self._subs.append(sub)  # type: ignore[arg-type]
        self._tasks.append(asyncio.create_task(self._consume(sub, account_id), name=f"persist-{account_id}"))

    async def _consume[E](self, sub: Subscription[E], account_id: str | None) -> None:
        async for event in sub:
            if isinstance(event, TickEvent | BarEvent):
                continue
            try:
                ts = getattr(event, "ts", None)
                when = ts if isinstance(ts, datetime) else self._clock.now()
                acct = getattr(event, "account_id", None) or account_id
                self.writer.submit(event_record(event, acct if isinstance(acct, str) else None, when))
            except Exception:
                _log.exception("persist_event_failed", kind=type(event).__name__)

    def put(self, kind: str, account_id: str | None, payload: object) -> None:
        try:
            self.writer.submit(record(kind, account_id, self._clock.now(), payload))
        except Exception:
            _log.exception("persist_record_failed", kind=kind)

    async def stop(self) -> None:
        """Drain what the subscriptions already received, then flush the writer (DB or WAL)."""
        for sub in self._subs:
            while (event := sub.get_nowait()) is not None:
                if not isinstance(event, TickEvent | BarEvent):
                    acct = getattr(event, "account_id", None)
                    ts = getattr(event, "ts", None)
                    self.writer.submit(
                        event_record(
                            event,
                            acct if isinstance(acct, str) else None,
                            ts if isinstance(ts, datetime) else self._clock.now(),
                        )
                    )
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.writer.stop()
