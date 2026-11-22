# ADR 0009 — Strategy framework and `trader run`

Status: accepted (phase 7). Code: `src/trader/strategy/`, `src/trader/brokers/feeds.py`, `src/trader/app.py`
(`App`, `run_app`), `trader run`. Spec: SPEC §8, §15.

## Strategies are pure

A strategy subclasses `Strategy[P]`, which declares `version` and a pydantic `Params` model. Every hook is
optional:
- `on_start`, `on_tick`, `on_bar` (complete bars only);
- `on_order` / `on_position` (its own orders and positions only);
- `on_timer`, `on_stop`.

Hooks are **synchronous**. A strategy reads the world only through its `StrategyContext`:
- clock and market data: `now()`, `bars`, `last_tick`, `instrument`;
- account: `account()`, its own `positions()` / `orders()`;
- managed `indicator(...)`s;
- helpers: `size_by_risk`, `params`, `state`, `log`;
- `random`, a `Random` seeded from config.

It acts only by queueing commands: `buy` / `sell` / `place` return the `OrderIntent`, and `modify`,
`cancel`, `close`, `close_all`, `modify_position`, `set_timer` are queued too. There is no wall clock, IO
or broker access, so the same code will run in the backtester. The `StrategyEnv` protocol (now, bars,
last tick, account view) is the only seam the backtester has to replace.

Indicators (`strategy/indicators/`) are incremental and work on `Decimal`: SMA, EMA, WMA, RSI, ATR, MACD,
Bollinger Bands, Stochastic, ADX, Donchian, SuperTrend, session VWAP, and classic pivot points.
- Each has `update(bar)`, plus `update_price` for single-price indicators; `value`; and `ready`.
- Updates are O(1). Rolling highs and lows use monotonic deques (amortized O(1)).
- Tests compare every indicator at every step with an independent batch implementation written from the
  textbook definition, over random series, plus hand-computed values.
- `ctx.indicator(kind, symbol, tf, **params)` creates one instance per set of arguments and warms it from
  the bar history. The runner then updates it with each complete bar before `on_bar`. Bars already seen are
  skipped, so history and live bars cannot double-count.

## Runner (`strategy/runner.py`)

**Lifecycle.** LOADED → WARMING (restore `ctx.state`, then `on_start`) → RUNNING ↔ PAUSED → STOPPED.
`start` / `pause` / `resume` / `stop` / `replace` work while the process runs, so the phase 10 API can call
them. A params change means a new instance: `replace` validates the new params first, stops the old
instance cleanly, and the state carries over.

**Isolation.** Every callback is wrapped:
- **On an exception** the callback is rolled back. The commands it queued are dropped and `ctx.state`
  goes back to its last saved value, so a half-done decision never reaches the OMS.
- **Errors are counted.** After `max_errors` errors only that strategy pauses; resuming it resets the count.
- **State must be JSON-serialisable.** Anything else counts as an error.
- **Slow callbacks are logged.** A callback over `callback_budget_ms` produces a `StrategyEvent("slow_callback")`.
  A synchronous callback cannot be interrupted, so the budget is a warning, not a limit.

**Routing.**
- Bars and ticks are routed by (account feed, symbol, timeframe).
- Order and position events go only to the owning strategy, matched by `strategy_id`.
- A paused strategy still feeds its indicators, so it can resume without a gap.

**Ownership.** Commands go through the account's `OrderManager`, so new orders go through risk. A
strategy can only modify, cancel or close its own orders and positions; anything else is rejected and
counted as an error. Broker or OMS refusals become `StrategyEvent("command_failed")`; they are not
counted as strategy errors.

**Kill switch.**
- A `RiskStateEvent` at PAUSE_NEW or above pauses every running strategy on that account.
- They resume when the level returns to NONE.
- Strategies paused by hand or by errors stay paused.
- A strategy started while the switch is on starts PAUSED.

**State.**
- `ctx.state` is saved after each callback that changed it, to `data/strategies/<id>.json` (atomic,
  0600).
- Writes happen off the event loop: at most one per 200 ms, and `stop` waits for any write in progress.
- The state is restored before `on_start`.
- Timers are not persisted: `on_start` sets them again.

## `trader run` (`App`)

Startup follows SPEC §15:
1. Start market data.
2. Connect the data-source feeds.
3. For each trading account: connect, reconcile, and warm up bars. Warm-up uses the most bars any strategy
   on the account needs, or `market_data.warmup_bars`.
4. Load strategies from `strategies:` in the config and start them.

Shutdown (SIGINT/SIGTERM, or `--duration`):
1. Pause strategies.
2. Wait, bounded, until no order is NEW, PENDING_SUBMIT or UNKNOWN.
3. Run `on_stop` and persist.
4. Stop the OMS, which persists.
5. Disconnect.

Any order still in flight stays persisted and is resolved on the next start (ADR 0007).

The process writes a run report to `data/reports/run-<ts>.json`:
- per strategy: status, callbacks, intents, errors, slow callbacks;
- per account: kill level, positions, orders by status.

`trader run` exits 1 if any strategy raised an error, which turns the phase 7 acceptance ("24 h without
errors") into a pass/fail check.

**Market data is keyed by account id.** Two accounts on the same broker type therefore have separate bars
and separate recorded ticks (`data/ticks/<account>/...`).

**Paper accounts get prices from one of two sources:**
- `market_data_from: <account>`: a demo or live account wrapped in `BrokerFeed`. It is used for market
  data only; the process never trades on it, and config validation refuses strategies on it.
- `synthetic_feed`: a seeded random walk (`SyntheticFeed`), so paper mode runs without credentials. It is
  **not market data** and has no candle history; strategies warm up from live bars.

Both feeds fan ticks out to every consumer.

**Config checks.** Strategy ids must be unique. A strategy's account must exist and must not be a data
source, its symbols must belong to that account, and its timeframes must be listed in
`market_data.timeframes`. Params are validated by the strategy class when it loads, and unknown keys are
errors.

## Example strategies (`strategy/examples/`)

These exercise the framework; they are not trading advice.
- `ema_cross`: EMA crossover with ATR stop and target. An opposite cross closes the position first.
- `donchian_breakout`: a close beyond the previous N-bar channel, with an ATR stop and a `TimeExit`.
- `rsi_reversion`: RSI crossing back over oversold/overbought, with an ATR stop and a `ScaleOut` ladder at
  k × ATR.

All three size with `ctx.size_by_risk` (% of equity at the stop, rounded down). They keep one position per
symbol and never add to an open one.

`tests/strategy_harness.py` drives a strategy bar by bar, with no broker and no clock, and returns the
commands each callback queued. The example tests use it.

## Acceptance: 24 h on paper and demo

These commands produce the evidence:
- `uv run trader run --config config/paper.yaml --duration 24h`: synthetic prices, no credentials needed.
- `uv run trader run --config config/demo.yaml --duration 24h`: demo accounts, needs `.env`.

Each prints the run summary, writes the report and exits non-zero on any strategy error. The tests run
the whole `App` on second bars: three strategies trade, shut down, persist, and restart with their state.
A paper account fed by the cTrader fake shows that `BrokerFeed` works without ever trading on the data
account.

## Consequences

- **Every callback runs on the event loop.** A strategy that does heavy computation in a callback delays
  everything on the loop. The budget warning makes this visible; moving strategies to worker processes is
  future work.
- **Synthetic-feed runs warm up live.** They have no candle history, so long indicators need hours before
  the first signal. Demo-fed runs warm up from broker candles.
