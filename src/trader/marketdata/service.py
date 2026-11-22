"""MarketDataService: the market-data side of the hot path (SPEC §2, §11).

``on_tick`` is synchronous and IO-free: publish to the tick bus (last-tick cache + subscribers), update
the feed-health monitor, update bars and publish completed bars. Recording, the timer that closes bars and
detects stale feeds, and UI streams are separate consumers, so none of them can slow the trading path.

Per broker: one ``BarBuilder`` and one ``StaleFeedMonitor`` (two brokers quoting the same symbol are
different feeds with different candles).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from trader.brokers.base import Broker
from trader.config.settings import MarketDataConfig
from trader.domain.enums import Timeframe
from trader.domain.events import BarEvent, DomainEvent, FeedEvent, TickEvent
from trader.domain.models import Bar, Instrument, Tick
from trader.domain.time import Clock
from trader.observability.logging import get_logger

from .bars import BarBuilder, PriceSource
from .bus import Bus, Overflow, TickBus
from .recorder import CandleStore, TickRecorder
from .stale import StaleFeedMonitor

_log = get_logger("trader.marketdata")


class MarketDataService:
    def __init__(self, config: MarketDataConfig, clock: Clock, data_dir: Path) -> None:
        self.config = config
        self._clock = clock
        self.ticks = TickBus()
        self.bars: Bus[BarEvent] = Bus()
        self.feeds: Bus[FeedEvent] = Bus()
        self._builders: dict[str, BarBuilder] = {}
        self._monitors: dict[str, StaleFeedMonitor] = {}
        self.candles = CandleStore(data_dir)
        self.recorder: TickRecorder | None = (
            TickRecorder(
                data_dir,
                clock,
                batch_size=config.recorder.batch_size,
                flush_interval=timedelta(seconds=config.recorder.flush_interval_s),
            )
            if config.recorder.enabled
            else None
        )
        self._tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------ setup

    def builder(self, broker: str) -> BarBuilder:
        b = self._builders.get(broker)
        if b is None:
            b = self._builders[broker] = BarBuilder(
                self.config.timeframes,
                price=PriceSource(self.config.price_source),
                day_offset=timedelta(minutes=self.config.day_offset_minutes),
                history=self.config.bar_history,
                grace=timedelta(milliseconds=self.config.bar_grace_ms),
            )
        return b

    def monitor(self, broker: str) -> StaleFeedMonitor:
        m = self._monitors.get(broker)
        if m is None:
            s = self.config.stale
            m = self._monitors[broker] = StaleFeedMonitor(
                broker,
                min_silence=timedelta(seconds=s.min_silence_s),
                max_silence=timedelta(seconds=s.max_silence_s),
                multiplier=s.multiplier,
            )
        return m

    def add_instruments(self, broker: str, instruments: Sequence[Instrument]) -> None:
        now = self._clock.now()
        for inst in instruments:
            self.monitor(broker).watch(inst, now)
            if self.recorder is not None:
                self.recorder.register(broker, inst)

    async def warm_up(
        self, broker_name: str, broker: Broker, symbol: str, bars: int | None = None
    ) -> dict[Timeframe, int]:
        """Seed bar history from ``broker.candles`` (and store the candles for the backtester).
        Timeframes the broker cannot provide are skipped (they warm up from live ticks)."""
        count = self.config.warmup_bars if bars is None else bars
        now = self._clock.now()
        loaded: dict[Timeframe, int] = {}
        if count <= 0:
            return loaded
        builder = self.builder(broker_name)
        for tf in self.config.timeframes:
            duration = tf.duration
            depth = broker.capabilities.candle_history.get(tf)
            if duration is None or depth is None:
                continue
            start = max(now - duration * (count + 1), now - depth)  # count complete bars + the forming one
            try:
                candles = await broker.candles(symbol, tf, start, now)
            except Exception as exc:  # warm-up is best effort; live ticks still build bars
                _log.warning(
                    "warmup_failed", broker=broker_name, symbol=symbol, timeframe=tf.value, error=str(exc)
                )
                continue
            builder.seed(candles)
            await self.candles.save(broker_name, candles)
            loaded[tf] = len(candles)
        return loaded

    # ------------------------------------------------------------------ hot path

    def on_tick(self, broker: str, tick: Tick) -> list[Bar]:
        before = self.ticks.last(tick.symbol, broker)
        self.ticks.publish(TickEvent(tick=tick, broker=broker))
        if before is not None and tick.ts < before.ts:
            return []  # out of order: the bus dropped it, so do the bars
        feed = self.monitor(broker).observe(tick)
        if feed is not None:
            self.feeds.publish(feed)
        done = self.builder(broker).on_tick(tick)
        for bar in done:
            self.bars.publish(BarEvent(bar=bar, broker=broker))
        return done

    def on_event(self, event: DomainEvent) -> bool:
        """Route a broker event; returns True if it was market data."""
        if isinstance(event, TickEvent):
            self.on_tick(event.broker, event.tick)
            return True
        return False

    def on_time(self) -> None:
        """Close bars whose time has passed and detect stale feeds (call about once a second)."""
        now = self._clock.now()
        for broker, builder in self._builders.items():
            for bar in builder.on_time(now):
                self.bars.publish(BarEvent(bar=bar, broker=broker))
        for monitor in self._monitors.values():
            for event in monitor.check(now):
                _log.warning(
                    "feed_stale", broker=event.broker, symbol=event.symbol, silence_ms=event.silence_ms
                )
                self.feeds.publish(event)

    # ------------------------------------------------------------------ queries

    def last_tick(self, symbol: str, broker: str | None = None) -> Tick | None:
        return self.ticks.last(symbol, broker)

    def history(self, broker: str, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        return self.builder(broker).history(symbol, tf, n)

    def current_bar(self, broker: str, symbol: str, tf: Timeframe) -> Bar | None:
        return self.builder(broker).current(symbol, tf)

    # ------------------------------------------------------------------ lifecycle

    async def start(self, *, tick_interval: timedelta = timedelta(seconds=1)) -> None:
        if self._tasks:
            return
        self._tasks.append(asyncio.create_task(self._timer(tick_interval), name="marketdata-timer"))
        if self.recorder is not None:
            sub = self.ticks.subscribe("tick-recorder", policy=Overflow.LOSSLESS, maxsize=200_000)
            self._tasks.append(asyncio.create_task(self.recorder.run(sub), name="tick-recorder"))

    async def _timer(self, interval: timedelta) -> None:
        while True:
            await asyncio.sleep(interval.total_seconds())
            try:
                self.on_time()
            except Exception:
                _log.exception("marketdata_timer_error")

    async def stop(self) -> None:
        """Stop timers, close subscriptions and flush the recorder."""
        if self._tasks:
            timer, *rest = self._tasks
            timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer
        else:
            rest = []
        self.ticks.close()  # ends the recorder loop, which flushes on exit
        for task in rest:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, 30)
        self._tasks = []
        self.bars.close()
        self.feeds.close()
