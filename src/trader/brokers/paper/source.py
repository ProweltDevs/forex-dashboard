"""A scriptable in-memory ``MarketDataSource`` for tests, demos and the contract suite."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from trader.domain.enums import Timeframe
from trader.domain.errors import UnknownSymbol
from trader.domain.models import Bar, Instrument, Tick
from trader.domain.time import Clock, ensure_utc, from_epoch_ms, to_epoch_ms

_CLOSED = object()


class FakeTickSource:
    """Ticks are pushed by the test (``push``/``push_quote``).

    Candles behave like a broker's: when ticks were pushed inside the requested window they are aggregated
    from those ticks (bid OHLC, tick-count volume, UTC boundaries, no bars without ticks). Otherwise they are
    synthetic flat bars at the last known price (or ``base_prices``), enough for warm-up plumbing."""

    def __init__(
        self,
        instruments: Sequence[Instrument],
        clock: Clock,
        *,
        base_prices: dict[str, Decimal] | None = None,
        history: int = 200_000,
    ) -> None:
        self._instruments = {i.symbol: i for i in instruments}
        self._clock = clock
        self._queue: asyncio.Queue[Tick | object] = asyncio.Queue()
        self._subscribed: set[str] = set()
        self._last: dict[str, Tick] = {}
        self._base = dict(base_prices or {})
        self._consumed = False
        self._history: dict[str, deque[Tick]] = {}
        self._history_len = history

    async def instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    async def subscribe(self, symbols: Sequence[str]) -> None:
        unknown = [s for s in symbols if s not in self._instruments]
        if unknown:
            raise UnknownSymbol(f"unknown symbols: {unknown}")
        self._subscribed.update(symbols)

    @property
    def subscribed(self) -> frozenset[str]:
        return frozenset(self._subscribed)

    def push(self, tick: Tick) -> None:
        if tick.symbol not in self._instruments:
            raise UnknownSymbol(tick.symbol)
        self._last[tick.symbol] = tick
        self._history.setdefault(tick.symbol, deque(maxlen=self._history_len)).append(tick)
        self._queue.put_nowait(tick)

    def push_quote(self, symbol: str, bid: Decimal, ask: Decimal, ts: datetime | None = None) -> Tick:
        now = ensure_utc(ts) if ts is not None else self._clock.now()
        tick = Tick(symbol=symbol, bid=bid, ask=ask, ts=now, recv_ts=now, recv_mono_ns=time.monotonic_ns())
        self.push(tick)
        return tick

    def close(self) -> None:
        self._queue.put_nowait(_CLOSED)

    async def ticks(self) -> AsyncIterator[Tick]:
        if self._consumed:
            raise RuntimeError("FakeTickSource.ticks() supports a single consumer")
        self._consumed = True
        while True:
            item = await self._queue.get()
            if item is _CLOSED:
                return
            assert isinstance(item, Tick)
            yield item

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        if symbol not in self._instruments:
            raise UnknownSymbol(symbol)
        duration = tf.duration
        if duration is None:
            return []  # calendar timeframes are not synthesised
        from_ticks = self._candles_from_ticks(symbol, tf, duration, start, end)
        if from_ticks:
            return from_ticks
        last = self._last.get(symbol)
        price = last.mid if last is not None else self._base.get(symbol, Decimal(1))
        price = self._instruments[symbol].round_price(price)
        step_ms = int(duration / timedelta(milliseconds=1))
        first = -(-to_epoch_ms(start) // step_ms) * step_ms  # ceil to the timeframe grid
        end_ms = to_epoch_ms(end)
        now_ms = to_epoch_ms(self._clock.now())
        return [
            Bar(
                symbol=symbol,
                timeframe=tf,
                start=from_epoch_ms(ms),
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Decimal(0),
                complete=ms + step_ms <= now_ms,
            )
            for ms in range(first, end_ms, step_ms)
        ]

    def _candles_from_ticks(
        self, symbol: str, tf: Timeframe, duration: timedelta, start: datetime, end: datetime
    ) -> list[Bar]:
        step_ms = int(duration / timedelta(milliseconds=1))
        lo, hi = to_epoch_ms(start), to_epoch_ms(end)
        buckets: dict[int, list[Decimal]] = {}
        for tick in self._history.get(symbol, ()):
            ms = to_epoch_ms(tick.ts)
            bucket = ms // step_ms * step_ms
            if lo <= bucket < hi:
                buckets.setdefault(bucket, []).append(tick.bid)
        now_ms = to_epoch_ms(self._clock.now())
        return [
            Bar(
                symbol=symbol,
                timeframe=tf,
                start=from_epoch_ms(ms),
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=Decimal(len(prices)),
                complete=ms + step_ms <= now_ms,
            )
            for ms, prices in sorted(buckets.items())
        ]
