# ADR 0003 — Broker interface, registry and paper broker

Status: accepted (phase 1)

## Broker port (`brokers/base.py`)

- `Broker` is a `runtime_checkable` `Protocol` matching SPEC §4. `name` and `capabilities` are read-only
  properties. Value objects: `Capabilities` (adds `oco`), `BrokerAck` (`duplicate=True` on an idempotent
  replay), `OrderChanges` (`None` = unchanged; `remove_sl`/`remove_tp` clear), `RateLimit`/`RateLimits`.
- `MarketDataSource` is a second, smaller port (instruments, subscribe, ticks, candles). The paper broker
  depends on it, so it can be fed by a real adapter (paper mode), `FakeTickSource` (tests), or a recorded-tick
  replay (the future backtester) without changes.
- **Error contract.** Request validation fails synchronously with `domain.errors` (`InvalidVolume`,
  `InvalidPrice`, `UnknownSymbol`, `MarketClosed`, `OrderNotFound`, `PositionNotFound`, `BrokerUnavailable`,
  `RateLimited`). Execution outcomes (fill, reject for margin or slippage, expiry) arrive as events.
  New errors: `UnknownSymbol`, `OrderNotFound`, `PositionNotFound`, `ConversionUnavailable`.
- `brokers/ratelimit.py`: a token bucket in `Decimal` on an injectable monotonic-ns clock. Real adapters
  `await acquire()` (client-side throttling); the paper broker uses `try_acquire()` to emulate a broker that
  rejects with `RateLimited(retry_after_ms=…)`.

## Registry (`brokers/registry.py`)

`BrokerRegistry` maps the config `broker` name to a factory `(AccountConfig, BrokerDeps) -> Broker`.
`default_registry()` registers `paper`, `ctrader` and `exness` (phases 2–3 added the last two; each
adapter is imported lazily, only when an account uses it).
`BrokerDeps` carries the clock, the env mapping (for `secrets_ref`) and, for paper, a `MarketDataSource`.

## Paper broker (`brokers/paper/`)

Behaves like the strictest real broker so the OMS is built against realistic semantics:

- `sync_order_result=False`: ACK first, `ACCEPTED` → (`WORKING`) → `FILLED`/`REJECTED`/`EXPIRED` via events.
- **No replay.** While disconnected and after reconnect **until `snapshot()`**, it emits no trade-related events,
  but keeps executing broker-side on ticks (an SL can hit while we are offline). Ticks and connection events
  are not gated. Initial `connect()` also waits for a snapshot, matching the startup sequence.
- Idempotency: a repeated `client_order_id` returns the original `broker_order_id` with `duplicate=True`.
- Fills: market and stop orders at bid/ask ± `slippage_points` (adverse); limits at the touch (no slippage).
  `max_slippage` smaller than the configured slippage rejects. `extra_spread_points` widens the quote.
  Pending orders must rest on the correct side of the market (as forex brokers enforce); IOC/FOK are
  market-only. STOP_LIMIT arms on the stop, then rests as a limit.
- SL/TP: checked on each tick against the closing price (bid for longs, ask for shorts). When a gap crosses
  both, SL wins (conservative). SL fills take slippage; TP fills don't.
- Hedging or netting (`hedging: false`): netting adds with a weighted entry, reduces, closes or reverses.
- Money: P&L in quote currency converted to account currency at the conversion pair's **mid**; margin is
  `units/leverage` in base currency for FX and `units*price/leverage` in quote currency for CFDs/metals.
  Commission per lot per side; swap per lot per night at `rollover_hour_utc` (weekdays only, triple on
  Wednesday). Orders whose P&L/margin cannot be converted (no live conversion tick) are refused.
- Time: `max(clock.now(), latest tick ts)`, so the same code runs on wall-clock or simulated time.
  Broker IDs are sequential (`PO…`, `PP…`, `PD…`) for deterministic tests.

Not native (the OMS emulates them, SPEC §6): trailing stop, OCO, close-by. GTD/DAY expiry is server-side.

## Contract suite (`tests/contract/`)

Written only against the `Broker` protocol plus a small `ContractHarness` (event recorder, forced
disconnect/reconnect, cleanup that cancels our orders and closes positions our orders opened). Covers every
SPEC §4 step. It is green on paper. Phase 2/3 adapters add a harness and a `pytest.param(..., marks=pytest.mark.demo)`
entry in `tests/contract/conftest.py`.
