from __future__ import annotations

import asyncio
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.factories import eurusd
from trader.brokers.paper import FakeTickSource, PaperBroker, PaperSettings
from trader.config.settings import AppConfig, MarketDataConfig, RecorderConfig, StaleFeedConfig
from trader.domain import Bar, BarEvent, FeedEvent, FeedState, Tick, TickEvent, Timeframe
from trader.domain.time import FixedClock
from trader.marketdata import CandleStore, MarketDataService, TickBus, TickRecorder, bar_start, read_ticks
from trader.marketdata import recorder as rec_mod

D = Decimal
T0 = datetime(2026, 1, 5, 23, 59, 58, tzinfo=UTC)  # Monday, just before UTC midnight


def tick(
    ts: datetime, bid: str, ask: str, *, broker_ts: datetime | None = None, symbol: str = "EURUSD"
) -> Tick:
    return Tick(
        symbol=symbol,
        bid=D(bid),
        ask=D(ask),
        ts=ts,
        recv_ts=ts + timedelta(milliseconds=3),
        recv_mono_ns=0,
        broker_ts=broker_ts,
    )


# ------------------------------------------------------------------ recorder


async def test_flush_partitions_by_utc_day_and_reads_back_exactly(tmp_path: Path) -> None:
    clock = FixedClock(T0)
    rec = TickRecorder(tmp_path, clock)
    rec.register("ctrader", eurusd())
    ticks = [
        tick(T0, "1.10001", "1.10011", broker_ts=T0),
        tick(T0 + timedelta(milliseconds=1999), "1.10002", "1.10012"),  # 23:59:59.999
        tick(T0 + timedelta(seconds=2), "1.100025", "1.10013"),  # 00:00:00 next day, 6 decimals
    ]
    for t in ticks:
        rec.append("ctrader", t)
    assert rec.pending == 3
    await rec.flush()
    base = tmp_path / "ticks" / "ctrader" / "EURUSD"
    assert sorted(p.name.split(".part-")[0] for p in base.glob("*.part-*.parquet")) == [
        "2026-01-05",
        "2026-01-06",
    ]
    back = read_ticks(tmp_path, "ctrader", "EURUSD", T0, T0 + timedelta(days=1))
    assert [(t.ts, t.bid, t.ask, t.broker_ts, t.recv_ts) for t in back] == [
        (t.ts, t.bid, t.ask, t.broker_ts, t.recv_ts) for t in ticks
    ]
    assert back[2].bid == D("1.100025")  # finer than instrument digits: kept exactly
    schema = pq.read_schema(next(base.glob("2026-01-05.part-*.parquet")))
    assert schema.metadata[b"trader.schema"] == b"ticks/1"
    assert str(schema.field("bid").type) == "decimal128(18, 5)"


async def test_compaction_merges_sorts_and_dedupes(tmp_path: Path) -> None:
    clock = FixedClock(T0)
    rec = TickRecorder(tmp_path, clock)
    rec.append("p", tick(T0 + timedelta(milliseconds=500), "1.2", "1.3"))
    await rec.flush()
    rec.append("p", tick(T0, "1.1", "1.2"))
    await rec.flush()
    base = tmp_path / "ticks" / "p" / "EURUSD"
    part = next(base.glob("2026-01-05.part-*.parquet"))
    shutil.copy(part, base / "2026-01-05.part-ZZZZZZZZZZZZZZZZZZZZZZZZZZ.parquet")  # crash leftover duplicate
    assert await rec.compact_closed_days() == 0  # today: not compacted yet
    clock.advance(timedelta(days=1))
    assert await rec.compact_closed_days() == 1
    assert list(base.glob("*.part-*")) == []
    table = pq.read_table(base / "2026-01-05.parquet")
    assert table.column("ts_ms").to_pylist() == sorted(table.column("ts_ms").to_pylist())
    assert table.num_rows == 2
    # Late parts for an already-compacted day are merged into it.
    rec.append("p", tick(T0 + timedelta(milliseconds=100), "1.15", "1.25"))
    await rec.flush()
    await rec.compact_closed_days()
    assert len(read_ticks(tmp_path, "p", "EURUSD", T0, T0 + timedelta(seconds=1))) == 3


async def test_write_failure_keeps_rows_for_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rec = TickRecorder(tmp_path, FixedClock(T0))
    rec.append("p", tick(T0, "1.1", "1.2"))
    real = rec_mod._write_rows
    calls = {"n": 0}

    def flaky(*args: object) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full")
        real(*args)  # type: ignore[arg-type]

    monkeypatch.setattr(rec_mod, "_write_rows", flaky)
    await rec.flush()
    assert (rec.write_errors, rec.pending, rec.recorded) == (1, 1, 0)
    await rec.flush()
    assert (rec.pending, rec.recorded) == (0, 1)


async def test_run_consumes_bus_flushes_and_rolls_over(tmp_path: Path) -> None:
    clock = FixedClock(T0)
    bus = TickBus()
    rec = TickRecorder(tmp_path, clock, batch_size=2, flush_interval=timedelta(hours=1))
    sub = bus.subscribe("rec")
    task = asyncio.create_task(rec.run(sub, poll=timedelta(milliseconds=10)))
    bus.publish(TickEvent(tick=tick(T0, "1.1", "1.2"), broker="p"))
    bus.publish(TickEvent(tick=tick(T0 + timedelta(milliseconds=1), "1.1", "1.2"), broker="p"))
    for _ in range(50):
        if rec.recorded == 2:
            break
        await asyncio.sleep(0.01)
    assert rec.recorded == 2  # batch size reached
    clock.advance(timedelta(days=1))
    bus.publish(TickEvent(tick=tick(T0 + timedelta(days=1), "1.3", "1.4"), broker="p"))
    for _ in range(100):
        if (tmp_path / "ticks" / "p" / "EURUSD" / "2026-01-05.parquet").exists():
            break
        await asyncio.sleep(0.01)
    assert (tmp_path / "ticks" / "p" / "EURUSD" / "2026-01-05.parquet").exists()  # compacted at rollover
    bus.close()
    await asyncio.wait_for(task, 5)
    assert rec.recorded == 3  # final flush on close


async def test_candle_store_merges_complete_bars(tmp_path: Path) -> None:
    store = CandleStore(tmp_path)

    def bar(i: int, close: str, complete: bool = True) -> Bar:
        c = D(close)
        return Bar(
            symbol="EURUSD",
            timeframe=Timeframe.M1,
            start=T0 + timedelta(minutes=i),
            open=c,
            high=c,
            low=c,
            close=c,
            volume=D(3),
            complete=complete,
        )

    assert await store.save("ctrader", [bar(0, "1.1"), bar(1, "1.2"), bar(2, "1.3", complete=False)]) == 2
    await store.save("ctrader", [bar(1, "1.25"), bar(3, "1.4")])
    loaded = store.load("ctrader", "EURUSD", Timeframe.M1, T0, T0 + timedelta(hours=1))
    assert [(b.start, b.close) for b in loaded] == [
        (T0, D("1.1")),
        (T0 + timedelta(minutes=1), D("1.25")),
        (T0 + timedelta(minutes=3), D("1.4")),
    ]
    assert store.load("ctrader", "EURUSD", Timeframe.H1, T0, T0 + timedelta(days=1)) == []


# ------------------------------------------------------------------ service


def md_config(**kw: object) -> MarketDataConfig:
    fields: dict[str, object] = {
        "timeframes": (Timeframe.M1,),
        "stale": StaleFeedConfig(min_silence_s=5, max_silence_s=10),
        "recorder": RecorderConfig(enabled=False),
    }
    fields.update(kw)
    return MarketDataConfig.model_validate(fields)


async def test_service_hot_path_bars_and_feeds(tmp_path: Path) -> None:
    clock = FixedClock(datetime(2026, 1, 5, 8, tzinfo=UTC))
    md = MarketDataService(md_config(), clock, tmp_path)
    md.add_instruments("paper", [eurusd()])
    ticks_sub = md.ticks.subscribe("strategy")
    bars_sub = md.bars.subscribe("strategy-bars")
    feeds_sub = md.feeds.subscribe("risk")
    t0 = clock.now()
    md.on_tick("paper", tick(t0, "1.1", "1.2"))
    md.on_tick("paper", tick(t0 - timedelta(seconds=1), "9.9", "9.9"))  # out of order: ignored everywhere
    done = md.on_tick("paper", tick(t0 + timedelta(minutes=1), "1.2", "1.3"))
    assert [b.start for b in done] == [t0]
    assert len(ticks_sub) == 2
    bar_ev = bars_sub.get_nowait()
    assert isinstance(bar_ev, BarEvent)
    assert bar_ev.bar.high == D("1.1")
    assert bar_ev.broker == "paper"
    last = md.last_tick("EURUSD", "paper")
    assert last is not None
    assert last.bid == D("1.2")

    clock.advance(timedelta(minutes=2, seconds=5))  # past the 08:01 bar end + grace; silent > max_silence
    md.on_time()
    assert [b.bar.start for b in [bars_sub.get_nowait()]] == [t0 + timedelta(minutes=1)]
    stale = feeds_sub.get_nowait()
    assert isinstance(stale, FeedEvent)
    assert stale.state is FeedState.STALE
    md.on_tick("paper", tick(clock.now(), "1.25", "1.35"))
    live = feeds_sub.get_nowait()
    assert live is not None
    assert live.state is FeedState.LIVE
    assert md.on_event(TickEvent(tick=tick(clock.now(), "1.3", "1.4"), broker="paper"))
    assert md.history("paper", "EURUSD", Timeframe.M1)[-1].start == t0 + timedelta(minutes=1)


async def test_service_warm_up_from_broker_and_store(tmp_path: Path) -> None:
    clock = FixedClock(datetime(2026, 1, 5, 8, 0, 30, tzinfo=UTC))
    source = FakeTickSource([eurusd()], clock, base_prices={"EURUSD": D("1.10000")})
    broker = PaperBroker(source, PaperSettings(), clock)
    await broker.connect()
    md = MarketDataService(md_config(timeframes=(Timeframe.M1, Timeframe.H1, Timeframe.MN1)), clock, tmp_path)
    loaded = await md.warm_up("paper", broker, "EURUSD", bars=10)
    assert loaded == {Timeframe.M1: 11, Timeframe.H1: 11}  # MN1: no broker history -> skipped
    hist = md.history("paper", "EURUSD", Timeframe.M1)
    assert len(hist) == 10
    assert hist[-1].start == clock.now().replace(second=0) - timedelta(minutes=1)
    forming = md.current_bar("paper", "EURUSD", Timeframe.M1)
    assert forming is not None
    assert forming.start == clock.now().replace(second=0)
    stored = md.candles.load("paper", "EURUSD", Timeframe.M1, clock.now() - timedelta(hours=1), clock.now())
    assert len(stored) == 10
    await broker.close()


async def test_service_start_stop_records_ticks(tmp_path: Path) -> None:
    clock = FixedClock(datetime(2026, 1, 5, 8, tzinfo=UTC))
    md = MarketDataService(md_config(recorder=RecorderConfig(enabled=True, batch_size=1000)), clock, tmp_path)
    md.add_instruments("paper", [eurusd()])
    await md.start(tick_interval=timedelta(milliseconds=10))
    for i in range(5):
        md.on_tick("paper", tick(clock.now() + timedelta(milliseconds=i), "1.1", "1.2"))
    await asyncio.sleep(0.05)
    await md.stop()
    assert md.recorder is not None
    assert md.recorder.recorded == 5
    assert len(read_ticks(tmp_path, "paper", "EURUSD", clock.now(), clock.now() + timedelta(seconds=1))) == 5


def test_config_defaults_and_offset_semantics() -> None:
    cfg = AppConfig().market_data
    assert cfg.price_source == "bid"
    assert cfg.recorder.enabled
    with pytest.raises(ValueError, match="duplicate"):
        MarketDataConfig.model_validate({"timeframes": ["M1", "M1"]})
    ny = timedelta(minutes=-120)  # documented meaning: days and weeks start at 22:00 UTC
    ts = datetime(2026, 1, 7, 23, 30, tzinfo=UTC)  # Wednesday
    assert bar_start(ts, Timeframe.D1, ny) == datetime(2026, 1, 7, 22, tzinfo=UTC)
    assert bar_start(ts, Timeframe.W1, ny) == datetime(2026, 1, 4, 22, tzinfo=UTC)  # Sunday 22:00 forex open
