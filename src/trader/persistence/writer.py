"""DbWriter: the async background writer (SPEC §12, ADR 0011).

- ``submit(record)`` is synchronous and never blocks or fails: trading never waits on the database.
- A background task writes batches (up to ``batch_size`` records, or whatever is queued every
  ``flush_interval``) in one transaction each.
- **Outage:** when a write fails, the batch and everything queued after it go to the WAL file (fsync'd), an
  ``AlertEvent("db_down")`` is published, and the writer retries every ``retry_interval``. While the WAL
  holds anything, new records are appended to it too, so the database receives records in submission order.
- **Recovery:** the WAL is replayed oldest first in batches (idempotent: unique record ids), then cleared,
  and ``AlertEvent("db_recovered")`` is published. A WAL left by a previous process is replayed at start.
- ``stop()`` flushes: to the database if it is up, else to the WAL.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from trader.domain.events import AlertEvent, DomainEvent
from trader.domain.time import Clock
from trader.marketdata.bus import Bus
from trader.observability.logging import get_logger

from .serialize import Record
from .sink import Sink
from .wal import WalFile

_log = get_logger("trader.persistence.writer")


@dataclass(slots=True)
class WriterStats:
    submitted: int = 0
    written: int = 0  # straight to the database
    buffered: int = 0  # sent to the WAL
    replayed: int = 0  # from the WAL to the database
    failures: int = 0
    outages: int = 0


class DbWriter:
    def __init__(
        self,
        sink: Sink,
        wal_path: Path,
        clock: Clock,
        *,
        batch_size: int = 500,
        flush_interval: timedelta = timedelta(milliseconds=200),
        retry_interval: timedelta = timedelta(seconds=5),
    ) -> None:
        self._sink = sink
        self.wal = WalFile(wal_path)
        self._clock = clock
        self._batch = batch_size
        self._flush_interval = flush_interval
        self._retry = retry_interval
        self._queue: deque[Record] = deque()
        self._wake = asyncio.Event()
        self._wal_records = 0
        self._wal_failing = False
        self.down = False
        self.stats = WriterStats()
        self.events: Bus[DomainEvent] = Bus()
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ API

    def submit(self, record: Record) -> None:
        self._queue.append(record)
        self.stats.submitted += 1
        if len(self._queue) >= self._batch:
            self._wake.set()

    @property
    def pending(self) -> int:
        """Records not yet in the database (queued + in the WAL)."""
        return len(self._queue) + self._wal_records

    async def start(self) -> None:
        self._wal_records = await asyncio.to_thread(self.wal.count)
        if self._wal_records:
            _log.warning("db_wal_found", records=self._wal_records, path=str(self.wal.path))
        self._task = asyncio.create_task(self._run(), name="db-writer")

    async def stop(self, timeout: timedelta = timedelta(seconds=10)) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(timeout.total_seconds()):
                await self._cycle(final=True)
        if self._queue:  # timed out or the database is down: keep everything durable
            await self._to_wal(self._drain())
        await self._sink.close()
        self.events.close()

    # ------------------------------------------------------------------ loop

    async def _run(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(
                    (self._retry if self.down else self._flush_interval).total_seconds()
                ):
                    await self._wake.wait()
            self._wake.clear()
            try:
                await self._cycle()
            except Exception:  # never let the writer die: the next cycle retries
                _log.exception("db_writer_cycle_failed")

    async def _cycle(self, *, final: bool = False) -> None:
        async with self._lock:
            if self._wal_records or self.down:
                if self._queue:  # keep submission order: behind what is already buffered
                    await self._to_wal(self._drain())
                await self._replay()
                if self._wal_records:
                    return
            while self._queue:
                batch = [self._queue.popleft() for _ in range(min(self._batch, len(self._queue)))]
                if not await self._write(batch):
                    await self._to_wal(batch + self._drain())
                    return
                self.stats.written += len(batch)
                if not final:
                    await asyncio.sleep(0)  # let trading run between batches

    def _drain(self) -> list[Record]:
        """Take everything queued, synchronously: records submitted while a WAL write is awaited stay
        queued for the next cycle (taking a copy, awaiting, then clearing would lose them)."""
        items = list(self._queue)
        self._queue.clear()
        return items

    async def _write(self, batch: list[Record]) -> bool:
        try:
            await self._sink.write(batch)
        except Exception as exc:
            self.stats.failures += 1
            if not self.down:
                self.down = True
                self.stats.outages += 1
                self._alert(
                    "db_down", "critical", f"database write failed ({exc!r}); buffering to {self.wal.path}"
                )
            return False
        if self.down:
            self.down = False
            self._alert("db_recovered", "info", "database reachable again; buffered records replayed")
        return True

    async def _to_wal(self, records: list[Record]) -> None:
        if not records:
            return
        try:
            await asyncio.to_thread(self.wal.append, records)
        except OSError as exc:  # disk full / unwritable: keep them in memory, in order, and say so
            self._queue.extendleft(reversed(records))
            if not self._wal_failing:
                self._wal_failing = True
                self._alert(
                    "wal_write_failed", "critical", f"cannot write {self.wal.path}: {exc}; holding in memory"
                )
            return
        self._wal_failing = False
        self._wal_records += len(records)
        self.stats.buffered += len(records)

    async def _replay(self) -> None:
        if not self._wal_records:
            if self.down and not await self._write([]):
                return  # still down
            return
        records = await asyncio.to_thread(self.wal.read)
        for i in range(0, len(records), self._batch):
            batch = records[i : i + self._batch]
            if not await self._write(batch):
                return  # still down: the WAL stays as it is (already-replayed batches are idempotent)
            self.stats.replayed += len(batch)
        await asyncio.to_thread(self.wal.clear)
        _log.info("db_wal_replayed", records=len(records))
        self._wal_records = 0

    def _alert(self, kind: str, severity: str, message: str) -> None:
        log = _log.error if severity == "critical" else _log.info
        log(kind, message=message)
        self.events.publish(
            AlertEvent(account_id="*", kind=kind, severity=severity, message=message, ts=self._clock.now())
        )
