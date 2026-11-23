# ADR 0006 — Market data: tick bus, bars, stale feeds, tick recorder

Status: accepted (phase 4). Code: `src/trader/marketdata/`. Spec: SPEC §2, §11.

## Hot path

`MarketDataService.on_tick(broker, tick)` is synchronous and IO-free. It publishes to the tick bus, which
updates the last-tick cache and every subscriber's queue. It then updates the feed-health monitor and the
per-broker bar builder, and publishes completed bars. Everything slow is a separate consumer:
- the tick recorder;
- a 1 s timer that closes bars after silence and detects stale feeds;
- UI streams.

Two brokers quoting the same symbol are separate feeds, so each broker gets its own builder and monitor.

## Bus (`bus.py`)

`publish` never awaits. Every subscriber has its own queue with one of three overflow policies:
- **LOSSLESS** (OMS, strategies, recorder): never drops. Past `maxsize` it logs a lag warning once
  and keeps growing; a lagging consumer costs memory, not data.
- **DROP_OLDEST**: bounded, counts drops.
- **COALESCE** (UI): the latest item per key, e.g. one tick per (broker, symbol).

The tick bus drops out-of-order ticks (`ts` older than the last for that broker and symbol), so the cache
and consumers never move backwards.

## Bars (`bars.py`)

- Boundaries are exact in UTC: a bar covers `[start, start + duration)`, and a tick exactly at `start` opens
  the new bar.
- `day_offset` (config `day_offset_minutes`) aligns the trading day to the broker's. Every timeframe is
  computed on `ts − offset`, so H4, D1, W1 and MN1 line up with broker candles; weeks start on Monday of
  that shifted calendar. Use −120 for days and weeks starting at 22:00 UTC (NY close, or a UTC+2 server's
  midnight; weeks then open Sunday 22:00). The demo acceptance test prints the broker's offset, inferred
  from its D1 candles.
- Prices come from one side of the quote, **bid by default** (broker candles are bid-based). Volume is the
  tick count.
- There are no bars for silent periods, the same as cTrader and Exness.
- A bar completes when a tick for a later bar arrives, or on the timer once `end + grace` has passed (grace
  defaults to 2 s, for late ticks). Only complete bars are published, so `on_bar` only ever sees complete
  bars; `current()` shows the forming one.
- Late ticks (older than the forming bar or an already-closed bar) are counted and ignored. Closed bars are
  never rewritten.
- Warm-up seeds history from `broker.candles()`: N complete bars plus the forming one, which ticks keep
  extending. The candles are also stored for the backtester (`CandleStore`).

## Stale feeds (`stale.py`)

A feed is STALE when it has been silent longer than `clamp(multiplier × EWMA(inter-tick gap), min, max)`
**while its session is open** (`Instrument.is_open`), and LIVE again on the next tick. The threshold adapts
per symbol: busy pairs get `min`, quiet CFDs a longer threshold, and before warm-up it is `max`. Gaps longer
than `max` (weekends, for example) are not learned. The monitor uses the local receive time, so broker clock
drift doesn't matter, and all arithmetic is integer ms. It emits a new domain event, `FeedEvent`
(STALE/LIVE) for notifications, metrics, `/health` and the event store. The risk engine's stale-feed
breaker uses its own tick clock (`risk/market.py` `FeedClock`, ADR 0008), so risk does not depend on this
monitor's adaptive thresholds.

## Tick recorder (`recorder.py`)

- Layout: `data/ticks/{feed}/{symbol}/{yyyy-mm-dd}.parquet`, split by the UTC day of `ts`. The feed key is
  the account id that received the ticks (SPEC §11 says `{broker}`; one broker can have several accounts with
  different prices). A paper account priced by `market_data_from` records under its own id.
- **Synthetic prices are never recorded** (phase 12): the random-walk feed would otherwise pollute the
  dataset the backtester will replay as market data.
- **Crash safety (refines the SPEC):** a Parquet file is only valid once its footer is written, so one open
  file per day would lose the whole day on a crash. Each flush writes a complete part file
  (`{day}.part-{ulid}.parquet`, written to a temp name then renamed). Closed days are compacted into
  `{day}.parquet` (sorted by `ts` then `recv_ts`, exact duplicates removed in case a crash happened between
  rename and unlink, parts deleted) at startup and at each UTC rollover. Readers read the day file plus any
  parts, so data is visible from the first flush. A crash loses at most the unflushed batch (default
  5,000 ticks or 10 s).
- Schema `ticks/1`: `ts_ms`, `broker_ts_ms` (nullable) and `recv_ts_ms` as int64; `bid` and `ask` as
  `decimal128(18, digits)`. Prices are exact: the scale widens if a tick carries more decimals than the
  instrument. File metadata records broker, symbol, digits and schema version.
- Parquet encoding and file IO run in `asyncio.to_thread` on immutable row snapshots, with one writer at a
  time. This is the standard way to keep blocking file IO off the event loop (CLAUDE.md rule 7). No state
  is shared with the thread. A failed write puts the rows back for the next flush, and the failure is
  counted and logged.
- `read_ticks()` is the reader for the future backtester and tools.

## Acceptance: bars vs broker candles

`compare_bars()` matches bars by exact start time and applies this tolerance rule:
- **Within tolerance:** a matched bar is within tolerance when |ΔO|, |ΔH|, |ΔL| and |ΔC| are each at most
  **3 points** (1 point = tick size).
- **Hard failure:** our high above the broker high, or our low below the broker low, by more than **1 point**.
  We can miss extremes from a conflated or throttled stream (Exness documents up to one tick per 500 ms), but
  we can never go beyond the broker's range if the price side is right.
- **Pass:** at least 90 % of matched bars within tolerance, no extra bars, at most 1 missing bar, none
  outside.

The **3 points / 90 %** figures are initial values. Adjust them after the first demo runs, and record what was
measured here.

| Target | How | Result |
|---|---|---|
| paper | S1 bars over 4 s of live paper ticks vs the fake broker's tick-built candles; every test run | exact (0 pts), 3/3 bars, ~800 ticks |
| cTrader demo | M1 over 6 min (`TRADER_BAR_COMPARE_MINUTES`), `-m demo` | pending credentials |
| Exness demo | same | pending credentials |
