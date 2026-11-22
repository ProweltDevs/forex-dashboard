"""DbWriter + WAL with a fake sink: batching, outage buffering in order, recovery and replay, a WAL left by
a previous process, shutdown while down, torn WAL lines; and the record serializer."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tests.factories import intent
from trader.domain import AlertEvent, OrderEvent, OrderStatus, Side
from trader.domain.models import Order
from trader.domain.time import SystemClock
from trader.persistence.serialize import Record, event_record, record, to_json
from trader.persistence.wal import WalFile, record_from_wal
from trader.persistence.writer import DbWriter

T0 = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)


class FakeSink:
    def __init__(self) -> None:
        self.rows: list[str] = []
        self.batches: list[int] = []
        self.failing = False
        self.closed = False

    async def write(self, records: Sequence[Record]) -> None:
        if self.failing:
            raise ConnectionError("db is down")
        self.rows.extend(r.payload["n"] for r in records)  # type: ignore[misc]
        if records:
            self.batches.append(len(records))

    async def close(self) -> None:
        self.closed = True


def rec(n: int) -> Record:
    return record("test", "acct", T0, {"n": n})


def writer(tmp_path: Path, sink: FakeSink, **kw: object) -> DbWriter:
    opts: dict[str, object] = {
        "batch_size": 500,
        "flush_interval": timedelta(milliseconds=10),
        "retry_interval": timedelta(milliseconds=20),
    }
    opts.update(kw)
    return DbWriter(sink, tmp_path / "wal" / "db.wal", SystemClock(), **opts)  # type: ignore[arg-type]


async def until(check: object, timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not check():  # type: ignore[operator]
            await asyncio.sleep(0.01)


async def test_batches_in_submission_order(tmp_path: Path) -> None:
    sink = FakeSink()
    w = writer(tmp_path, sink)
    await w.start()
    for n in range(1200):
        w.submit(rec(n))
    await until(lambda: len(sink.rows) == 1200)
    assert sink.rows == list(range(1200))
    assert max(sink.batches) <= 500
    await w.stop()
    assert sink.closed
    assert (w.stats.written, w.stats.buffered, w.pending) == (1200, 0, 0)


async def test_outage_buffers_to_the_wal_in_order_and_replays_on_recovery(tmp_path: Path) -> None:
    sink = FakeSink()
    w = writer(tmp_path, sink)
    alerts: list[AlertEvent] = []
    sub = w.events.subscribe("t")
    collector = asyncio.create_task(_collect(sub, alerts))
    await w.start()
    for n in range(10):
        w.submit(rec(n))
    await until(lambda: len(sink.rows) == 10)
    sink.failing = True  # the database dies
    for n in range(10, 30):
        w.submit(rec(n))
    await until(lambda: w.down and w.wal.count() == 20)
    for n in range(30, 40):  # trading goes on: still accepted, behind the buffered ones
        w.submit(rec(n))
    await until(lambda: w.wal.count() == 30)
    assert sink.rows == list(range(10))
    sink.failing = False  # back
    await until(lambda: len(sink.rows) == 40 and not w.down)
    assert sink.rows == list(range(40))  # nothing lost, nothing reordered, nothing duplicated
    assert not w.wal.path.exists()
    assert (w.stats.outages, w.stats.replayed, w.pending) == (1, 30, 0)
    await w.stop()
    await asyncio.sleep(0.01)
    collector.cancel()
    assert [a.kind for a in alerts] == ["db_down", "db_recovered"]


async def _collect(sub: object, out: list[AlertEvent]) -> None:
    async for e in sub:  # type: ignore[attr-defined]
        out.append(e)


async def test_records_submitted_while_the_wal_is_being_written_are_not_lost(tmp_path: Path) -> None:
    """Regression: the queue used to be copied, the WAL write awaited, then the queue cleared."""
    import time

    sink = FakeSink()
    sink.failing = True
    w = writer(tmp_path, sink)
    slow_append = w.wal.append

    def append(records: Sequence[Record]) -> None:
        time.sleep(0.05)  # a slow disk: trading submits meanwhile
        slow_append(records)

    w.wal.append = append  # type: ignore[method-assign]
    await w.start()
    for n in range(200):
        w.submit(rec(n))
        await asyncio.sleep(0.001)
    await until(lambda: w.wal.count() == 200)
    sink.failing = False
    await until(lambda: len(sink.rows) == 200)
    assert sink.rows == list(range(200))
    await w.stop()


async def test_an_unwritable_wal_keeps_records_in_memory(tmp_path: Path) -> None:
    sink = FakeSink()
    sink.failing = True
    w = writer(tmp_path, sink)
    real_append = w.wal.append
    broken = {"on": True}

    def append(records: Sequence[Record]) -> None:
        if broken["on"]:
            raise OSError(28, "No space left on device")
        real_append(records)

    w.wal.append = append  # type: ignore[method-assign]
    await w.start()
    for n in range(20):
        w.submit(rec(n))
    await until(lambda: w.stats.failures >= 2)
    assert w.pending == 20  # nothing dropped
    broken["on"] = False
    sink.failing = False
    await until(lambda: len(sink.rows) == 20)
    assert sink.rows == list(range(20))
    await w.stop()


async def test_a_wal_left_by_a_previous_process_is_replayed_first(tmp_path: Path) -> None:
    WalFile(tmp_path / "wal" / "db.wal").append([rec(0), rec(1)])
    sink = FakeSink()
    w = writer(tmp_path, sink)
    await w.start()
    assert w.pending == 2
    w.submit(rec(2))
    await until(lambda: len(sink.rows) == 3)
    assert sink.rows == [0, 1, 2]
    await w.stop()


async def test_stop_while_down_keeps_everything_in_the_wal(tmp_path: Path) -> None:
    sink = FakeSink()
    sink.failing = True
    w = writer(tmp_path, sink, flush_interval=timedelta(seconds=10))
    await w.start()
    for n in range(5):
        w.submit(rec(n))
    await w.stop(timeout=timedelta(seconds=1))
    assert [r.payload["n"] for r in WalFile(tmp_path / "wal" / "db.wal").read()] == [0, 1, 2, 3, 4]


def test_wal_round_trip_and_torn_last_line(tmp_path: Path) -> None:
    wal = WalFile(tmp_path / "db.wal")
    wal.append([rec(1), rec(2)])
    with wal.path.open("a") as fh:
        fh.write('{"record_id": "01J')  # crash mid-write
    assert [r.payload["n"] for r in wal.read()] == [1, 2]
    r = rec(7)
    assert record_from_wal(r.to_wal()) == r
    wal.append([rec(3)])
    assert oct(wal.path.stat().st_mode & 0o777) == "0o600"  # may hold account data
    wal.clear()
    assert wal.read() == []


def test_serializer_encodes_domain_events() -> None:
    req = intent(sl=Decimal("1.09"), signal_ts=T0).to_request("01J00000000000000000000000")
    order = Order.new(req, T0)
    r = event_record(OrderEvent(account_id="a", order=order, previous_status=None, ts=T0), "a", T0)
    assert r.kind == "OrderEvent"
    assert len(r.record_id) == 26
    o = r.payload["order"]
    assert isinstance(o, dict)
    assert o["status"] == OrderStatus.NEW.value
    req_json = o["request"]
    assert isinstance(req_json, dict)
    assert (req_json["sl"], req_json["side"], req_json["signal_ts"]) == (
        "1.09",
        Side.BUY.value,
        1767693600000,
    )
    assert to_json({"d": timedelta(seconds=2), "t": (1, Decimal("0.1"))}) == {"d": 2000, "t": [1, "0.1"]}
