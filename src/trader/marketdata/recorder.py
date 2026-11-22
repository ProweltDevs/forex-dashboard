"""Tick recorder (always on) and candle store: the future backtest dataset (SPEC §11).

Layout: ``{root}/ticks/{broker}/{symbol}/{yyyy-mm-dd}.parquet`` (UTC day of the tick's ``ts``).

Crash safety: a Parquet file is only valid once its footer is written, so appending to one open file per day
would lose the whole day on a crash. Each flush therefore writes a complete, atomically renamed part file
(``{day}.part-{ulid}.parquet``); finished days are compacted into ``{day}.parquet`` (sorted, de-duplicated,
then the parts are removed). Readers always read the day file plus any parts. A crash loses at most the
unflushed batch.

Schema ``ticks/1``: ``ts_ms`` int64, ``broker_ts_ms`` int64 (nullable), ``recv_ts_ms`` int64, ``bid``/``ask``
``decimal128(18, digits)``: exact prices, epoch-ms times (CLAUDE.md storage rules).

File IO and Parquet encoding run in a worker thread (``asyncio.to_thread``) on immutable row snapshots so
the event loop never blocks; one writer at a time keeps parts ordered.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from trader.domain.enums import Timeframe
from trader.domain.events import TickEvent
from trader.domain.ids import new_client_order_id
from trader.domain.models import Bar, Instrument, Tick
from trader.domain.time import Clock, ensure_utc, from_epoch_ms, to_epoch_ms
from trader.observability.logging import get_logger

from .bus import Subscription, SubscriptionClosed

_log = get_logger("trader.marketdata.recorder")

TICK_SCHEMA_VERSION = "ticks/1"
CANDLE_SCHEMA_VERSION = "candles/1"
_PRECISION = 18

type _Row = tuple[int, int | None, int, Decimal, Decimal]


def _tick_schema(scale: int, broker: str, symbol: str) -> pa.Schema:
    dec: pa.DataType = pa.decimal128(_PRECISION, scale)
    i64: pa.DataType = pa.int64()
    fields: list[tuple[str, pa.DataType]] = [
        ("ts_ms", i64),
        ("broker_ts_ms", i64),
        ("recv_ts_ms", i64),
        ("bid", dec),
        ("ask", dec),
    ]
    return pa.schema(
        [pa.field(name, typ, nullable=name == "broker_ts_ms") for name, typ in fields],
        metadata={
            "trader.schema": TICK_SCHEMA_VERSION,
            "broker": broker,
            "symbol": symbol,
            "digits": str(scale),
        },
    )


def _scale_of(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    return -exponent if isinstance(exponent, int) and exponent < 0 else 0


def _atomic_write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def tick_dir(root: Path, broker: str, symbol: str) -> Path:
    return root / "ticks" / broker / symbol


@dataclass(slots=True)
class _Partition:
    rows: list[_Row] = field(default_factory=list)


class TickRecorder:
    def __init__(
        self,
        root: Path,
        clock: Clock,
        *,
        batch_size: int = 5_000,
        flush_interval: timedelta = timedelta(seconds=10),
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        self.root = root
        self._clock = clock
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._digits: dict[tuple[str, str], int] = {}
        self._pending: dict[tuple[str, str, date], _Partition] = {}
        self._pending_rows = 0
        self._lock = asyncio.Lock()
        self._last_flush = clock.now()
        self.recorded = 0
        self.files_written = 0
        self.write_errors = 0

    def register(self, broker: str, instrument: Instrument) -> None:
        self._digits[(broker, instrument.symbol)] = instrument.digits

    def append(self, broker: str, tick: Tick) -> None:
        """Buffer one tick (synchronous, no IO)."""
        key = (broker, tick.symbol, ensure_utc(tick.ts).date())
        part = self._pending.get(key)
        if part is None:
            part = self._pending[key] = _Partition()
        part.rows.append(
            (
                to_epoch_ms(tick.ts),
                to_epoch_ms(tick.broker_ts) if tick.broker_ts is not None else None,
                to_epoch_ms(tick.recv_ts),
                tick.bid,
                tick.ask,
            )
        )
        self._pending_rows += 1

    @property
    def pending(self) -> int:
        return self._pending_rows

    def should_flush(self) -> bool:
        return self._pending_rows >= self.batch_size or (
            self._pending_rows > 0 and self._clock.now() - self._last_flush >= self.flush_interval
        )

    async def flush(self) -> None:
        """Write every buffered partition as a new part file (off the event loop)."""
        async with self._lock:
            batches, self._pending, self._pending_rows = self._pending, {}, 0
            self._last_flush = self._clock.now()
            for (broker, symbol, day), part in batches.items():
                if not part.rows:
                    continue
                digits = self._digits.get((broker, symbol))
                scale = max(
                    [
                        digits or 0,
                        *(_scale_of(r[3]) for r in part.rows),
                        *(_scale_of(r[4]) for r in part.rows),
                    ]
                )
                path = (
                    tick_dir(self.root, broker, symbol)
                    / f"{day.isoformat()}.part-{new_client_order_id()}.parquet"
                )
                try:
                    await asyncio.to_thread(_write_rows, part.rows, scale, broker, symbol, path)
                except Exception:
                    # Put the rows back so the next flush retries; never lose recorded ticks silently.
                    self.write_errors += 1
                    _log.exception(
                        "tick_recorder_write_failed", broker=broker, symbol=symbol, rows=len(part.rows)
                    )
                    restored = self._pending.setdefault((broker, symbol, day), _Partition())
                    restored.rows[:0] = part.rows
                    self._pending_rows += len(part.rows)
                    continue
                self.recorded += len(part.rows)
                self.files_written += 1

    async def run(self, sub: Subscription[TickEvent], *, poll: timedelta = timedelta(seconds=1)) -> None:
        """Consume the tick bus until the subscription closes; flush by size or interval; compact
        closed days at startup and at each UTC day rollover."""
        await self.compact_closed_days()
        today = self._clock.now().date()
        try:
            while True:
                try:
                    event = await asyncio.wait_for(sub.get(), poll.total_seconds())
                    self.append(event.broker, event.tick)
                    while (more := sub.get_nowait()) is not None:  # drain without awaiting per tick
                        self.append(more.broker, more.tick)
                except TimeoutError:
                    pass
                if self.should_flush():
                    await self.flush()
                now_day = self._clock.now().date()
                if now_day != today:
                    today = now_day
                    await self.flush()
                    await self.compact_closed_days()
        except SubscriptionClosed:
            pass
        finally:
            with contextlib.suppress(Exception):
                await self.flush()

    async def compact_closed_days(self) -> int:
        """Merge part files of every day before today into its day file. Returns days compacted."""
        today = self._clock.now().date()
        return await asyncio.to_thread(_compact_tree, self.root / "ticks", today)


def _write_rows(rows: Sequence[_Row], scale: int, broker: str, symbol: str, path: Path) -> None:
    schema = _tick_schema(scale, broker, symbol)
    cols = list(zip(*rows, strict=True))
    table = pa.Table.from_arrays(
        [
            pa.array(cols[0], pa.int64()),
            pa.array(cols[1], pa.int64()),
            pa.array(cols[2], pa.int64()),
            pa.array(cols[3], schema.field("bid").type),
            pa.array(cols[4], schema.field("ask").type),
        ],
        schema=schema,
    )
    _atomic_write(table, path)


def _read_table(path: Path) -> pa.Table:
    return pq.read_table(path)


def _unify(tables: list[pa.Table]) -> pa.Table:
    """Concatenate day tables, widening price scale to the largest seen (exact)."""
    scale = max(t.schema.field("bid").type.scale for t in tables)
    meta = tables[-1].schema.metadata or {}
    target = _tick_schema(scale, meta.get(b"broker", b"").decode(), meta.get(b"symbol", b"").decode())
    return pa.concat_tables([t.cast(target) for t in tables])


def _compact_tree(ticks_root: Path, today: date) -> int:
    if not ticks_root.exists():
        return 0
    days: dict[Path, list[Path]] = {}
    for part in ticks_root.glob("*/*/*.part-*.parquet"):
        day_name = part.name.split(".part-", 1)[0]
        try:
            day = date.fromisoformat(day_name)
        except ValueError:
            continue
        if day < today:
            days.setdefault(part.parent / f"{day_name}.parquet", []).append(part)
    for day_file, parts in days.items():
        tables = ([_read_table(day_file)] if day_file.exists() else []) + [
            _read_table(p) for p in sorted(parts)
        ]
        merged = _unify(tables)
        # Sort by event time then receive time; drop exact duplicates (a crash between replace and unlink).
        merged = (
            merged.group_by(merged.column_names, use_threads=False)
            .aggregate([])
            .sort_by([("ts_ms", "ascending"), ("recv_ts_ms", "ascending")])
        )
        _atomic_write(merged.select(["ts_ms", "broker_ts_ms", "recv_ts_ms", "bid", "ask"]), day_file)
        for p in parts:
            p.unlink(missing_ok=True)
        _log.info("tick_day_compacted", file=str(day_file), parts=len(parts), rows=merged.num_rows)
    return len(days)


def read_ticks(root: Path, broker: str, symbol: str, start: datetime, end: datetime) -> list[Tick]:
    """Recorded ticks with ``start <= ts < end``, ordered by (ts, recv_ts). Synchronous: for the backtester,
    tools and tests (call via ``asyncio.to_thread`` from async code)."""
    start, end = ensure_utc(start), ensure_utc(end)
    base = tick_dir(root, broker, symbol)
    files: list[Path] = []
    day = start.date()
    while day <= end.date():
        files += [p for p in [base / f"{day.isoformat()}.parquet"] if p.exists()]
        files += sorted(base.glob(f"{day.isoformat()}.part-*.parquet"))
        day += timedelta(days=1)
    if not files:
        return []
    table = _unify([_read_table(f) for f in files])
    lo, hi = to_epoch_ms(start), to_epoch_ms(end)
    rows = sorted(
        (r for r in table.to_pylist() if lo <= r["ts_ms"] < hi), key=lambda r: (r["ts_ms"], r["recv_ts_ms"])
    )
    out: list[Tick] = []
    seen: set[tuple[object, ...]] = set()
    for r in rows:
        ident = (r["ts_ms"], r["broker_ts_ms"], r["recv_ts_ms"], r["bid"], r["ask"])
        if ident in seen:
            continue
        seen.add(ident)
        out.append(
            Tick(
                symbol=symbol,
                bid=r["bid"],
                ask=r["ask"],
                ts=from_epoch_ms(r["ts_ms"]),
                broker_ts=from_epoch_ms(r["broker_ts_ms"]) if r["broker_ts_ms"] is not None else None,
                recv_ts=from_epoch_ms(r["recv_ts_ms"]),
                recv_mono_ns=0,
            )
        )
    return out


# ------------------------------------------------------------------------------------------------ candles


class CandleStore:
    """Broker candles fetched for warm-up: ``{root}/candles/{broker}/{symbol}/{tf}.parquet``, merged by
    start time (newer data wins), complete bars only. Small files: rewritten atomically on each save."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, broker: str, symbol: str, tf: Timeframe) -> Path:
        return self.root / "candles" / broker / symbol / f"{tf.value}.parquet"

    async def save(self, broker: str, bars: Iterable[Bar]) -> int:
        complete = [b for b in bars if b.complete]
        by_key: dict[tuple[str, Timeframe], list[Bar]] = {}
        for b in complete:
            by_key.setdefault((b.symbol, b.timeframe), []).append(b)
        for (symbol, tf), group in by_key.items():
            await asyncio.to_thread(self._merge, self.path(broker, symbol, tf), group, broker)
        return len(complete)

    def _merge(self, path: Path, bars: list[Bar], broker: str) -> None:
        existing = (
            {b.start: b for b in self._load_path(path, bars[0].symbol, bars[0].timeframe)}
            if path.exists()
            else {}
        )
        existing.update({b.start: b for b in bars})
        ordered = [existing[k] for k in sorted(existing)]
        scale = max(_scale_of(v) for b in ordered for v in (b.open, b.high, b.low, b.close))
        dec: pa.DataType = pa.decimal128(_PRECISION, scale)
        vol: pa.DataType = pa.decimal128(_PRECISION, max(_scale_of(b.volume) for b in ordered))
        cols: list[tuple[str, pa.DataType]] = [
            ("start_ms", pa.int64()),
            ("open", dec),
            ("high", dec),
            ("low", dec),
            ("close", dec),
            ("volume", vol),
        ]
        schema = pa.schema(
            cols,
            metadata={
                "trader.schema": CANDLE_SCHEMA_VERSION,
                "broker": broker,
                "symbol": ordered[0].symbol,
                "timeframe": ordered[0].timeframe.value,
            },
        )
        table = pa.Table.from_pylist(
            [
                {
                    "start_ms": to_epoch_ms(b.start),
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": b.volume,
                }
                for b in ordered
            ],
            schema=schema,
        )
        _atomic_write(table, path)

    def _load_path(self, path: Path, symbol: str, tf: Timeframe) -> list[Bar]:
        rows = pq.read_table(path).to_pylist()
        return [
            Bar(
                symbol=symbol,
                timeframe=tf,
                start=from_epoch_ms(r["start_ms"]),
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                volume=r["volume"],
                complete=True,
            )
            for r in rows
        ]

    def load(self, broker: str, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        path = self.path(broker, symbol, tf)
        if not path.exists():
            return []
        start, end = ensure_utc(start), ensure_utc(end)
        return [b for b in self._load_path(path, symbol, tf) if start <= b.start < end]
