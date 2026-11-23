# Trader Platform — Product & Technical Specification

Status: v1 scope. Chart UI and backtesting come later but are designed for now.
Audience: Claude Code and the developer. Pair this with `CLAUDE.md` (rules) at the repo root.

---

## 1. Goals

1. Execute and manage orders through broker APIs directly: no MT5 terminal, low and measurable latency.
2. Run several automated strategies at once, across symbols and accounts, with strict risk control.
3. Give accurate real-time and historical P&L and performance analytics, including **execution quality**
   (slippage and latency). That's the original reason for leaving MT5.
4. Make brokers plug-and-play: Exness and cTrader first, more later (MT5 bridge, IG, FIX) with no core changes.
5. Survive crashes, restarts and disconnects without losing track of money or positions.

Non-goals (v1): chart UI, backtester, multi-user / SaaS, mobile apps, copy trading for other people.

---

## 2. System overview

```
             +-------------------------------- trader process (asyncio) -----------------------------+
 Control API |  FastAPI (REST + WS)  <---- reads ----  Portfolio / Analytics / Risk status           |
 (future UI) |        |  commands (start/stop strategy, manual order, kill switch)                    |
             |        v                                                                               |
             |  Strategy Runner --OrderIntent--> RiskEngine --approved--> OrderManager (per account) |
             |        ^  ticks/bars/events                                        |                   |
             |        |                                                           v                   |
             |  MarketData (tick bus, bar builder, recorder)  <---ticks---  Broker interface         |
             |                                                     events --> (Exness | cTrader |    |
             |  Persistence (Postgres + event store + Parquet ticks)                Paper)             |
             +----------------------------------------------------------------------------------------+
```

- **One process, many accounts.** Each `AccountRuntime` = one broker adapter instance + one OMS + one
  portfolio. An account belongs to exactly one broker.
- **In-process event bus** (asyncio queues, typed events). Each subscriber gets a bounded queue.
  Slow consumers (analytics, API) must never block the trading path: drop or coalesce for UI streams,
  never for the OMS.
- **Hot path:** tick → strategy → intent → risk → OMS → adapter. Keep it allocation-light and IO-free
  except the final broker call. Persistence happens asynchronously after events are published.

---

## 3. Domain model (`src/trader/domain/`)

Core types (frozen dataclasses, `Decimal` fields):

- `Instrument`: canonical symbol, broker symbol and ID, digits, tick size, contract size, min/max/step
  volume (lots), quote currency, margin currency, trading sessions, `hedging_allowed`.
- `Tick`: symbol, bid, ask, ts (UTC), broker ts, receive ts (local monotonic + UTC).
- `Bar`: symbol, timeframe, open/high/low/close, volume, start ts, `complete: bool`.
- `OrderRequest` / `OrderIntent`: side, type (`MARKET`, `LIMIT`, `STOP`, `STOP_LIMIT`), volume (lots),
  price(s), SL, TP, time-in-force/expiry, max slippage, `strategy_id`, `client_order_id`, tags.
- `Order`: OrderRequest + `status`, broker order ID, filled volume, avg fill price, timestamps of every
  state change, rejection reason.
- `Fill` / `Deal`: order ID, position ID, volume, price, commission, swap, ts.
- `Position`: ID, symbol, side, volume, entry price, SL, TP, unrealized P&L, opened ts, strategy ID.
  Supports both **hedging** (many positions per symbol) and **netting** (one per symbol) accounts.
- `AccountState`: balance, equity, margin, free margin, margin level, currency, leverage.
- Events: `TickEvent`, `BarEvent`, `OrderEvent(status change)`, `FillEvent`, `PositionEvent`,
  `AccountEvent`, `ConnectionEvent(up/down/degraded)`, `RiskEvent`, `StrategyEvent`.

### Order state machine (`oms/state_machine.py`)

```
NEW ─submit─> PENDING_SUBMIT ─ack─> ACCEPTED ─(pending types)─> WORKING
   PENDING_SUBMIT ─timeout─> UNKNOWN ─reconcile─> (any real state)
   ACCEPTED|WORKING ─partial─> PARTIALLY_FILLED ─> FILLED
   any non-terminal ─> CANCELLED | REJECTED | EXPIRED
Terminal: FILLED, CANCELLED, REJECTED, EXPIRED
```

Illegal transitions raise errors and are tested exhaustively. `UNKNOWN` is mandatory: a timed-out
submit is never assumed failed. Reconcile with the broker, using the same `client_order_id`.

---

## 4. Broker interface (`src/trader/brokers/base.py`)

```python
@dataclass(frozen=True)
class Capabilities:
    order_types: frozenset[OrderType]
    native_sl_tp: bool
    native_trailing_stop: bool
    partial_close: bool
    close_by: bool
    hedging: bool
    netting: bool
    modify_pending: bool
    server_side_expiry: bool
    tick_stream: bool
    candle_history: dict[Timeframe, timedelta]   # how far back each timeframe goes
    max_candles_per_request: int
    sync_order_result: bool                      # False = ACK first, result arrives later via events

class Broker(Protocol):
    name: str
    capabilities: Capabilities

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def instruments(self) -> list[Instrument]: ...
    async def account(self) -> AccountState: ...
    async def positions(self) -> list[Position]: ...
    async def open_orders(self) -> list[Order]: ...
    async def snapshot(self) -> BrokerSnapshot: ...            # account + positions + orders, for reconcile
    async def place_order(self, req: OrderRequest) -> BrokerAck: ...
    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck: ...
    async def cancel_order(self, order_id: str) -> BrokerAck: ...
    async def modify_position(self, position_id: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck: ...
    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck: ...
    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]: ...
    async def subscribe_ticks(self, symbols: Sequence[str]) -> None: ...
    def events(self) -> AsyncIterator[DomainEvent]: ...        # ticks, order/fill/position/account, connection
    async def limits(self) -> RateLimits: ...
```

Adapter responsibilities:
- Auth and token refresh, host discovery, connection lifecycle, heartbeats, reconnect with backoff + jitter.
- **Client-side rate limiting** (token bucket per endpoint class), using limits from the broker.
- Mapping symbols, units, prices, rounding (respect digits / tick size / volume step), errors.
- Emit a `ConnectionEvent` on every state change. After reconnect, emit nothing trade-related until the OMS
  has run `snapshot()`.
- Paginate history, chunking candle requests by `max_candles_per_request`.

**Registry:** `brokers/registry.py` maps `"exness" | "ctrader" | "paper"` to factories. Accounts are
configured in YAML (`broker: ctrader`, `account_id: ...`, `environment: demo|live`, `secrets_ref: ...`).

### Contract test suite (`tests/contract/`)

One parametrised suite run against every adapter (paper always; real adapters with `-m demo`):
connect → instruments → account → subscribe ticks (receive ≥ N ticks in T seconds) → market buy min volume →
fill event → modify SL/TP → partial close (if capable) → close → pending limit far from market → modify price →
cancel → candles for each supported timeframe → forced disconnect → reconnect → reconcile matches broker →
duplicate submit with the same `client_order_id` creates no second order → rate-limit handling.
**A broker is not "supported" until this suite is green on demo.**

---

## 5. Broker notes (verify every item against the official docs; record findings in `docs/brokers/*.md`)

### 5.1 Exness Trader API

- Docs: https://www.exness-api.com/reference/trading/exness-trader-api
  OpenAPI spec: https://www.exness-api.com/openapi/trading.public.yaml (generate request/response models from it).
- Works **only with Exness-platform accounts** (not MT4/MT5 accounts). Needs full KYC. Keys are created in
  Personal Area → Key Management (Account ID, API Key, Secret).
- **Auth:** signed requests (Ed25519 per docs). Keys can be scoped by account, source IP and operation type.
  A full-trading key covers exactly one trading account.
- **Host discovery:** use the Gateway host to resolve the account's **Access Point** host, then send trading
  calls there. Cache it as the docs allow and re-resolve on failure.
- **Async execution:** trading calls return an ACK ("accepted for processing"). The final result arrives as a
  WebSocket server event → `Capabilities.sync_order_result = False`. The OMS waits for the event, with a timeout
  that leads to `UNKNOWN` → reconcile.
- **Idempotency key** on trading requests (5–128 chars) → use `client_order_id`.
- **Streams:** server-events WebSocket (orders, positions, deals, account) and a separate **ticks WebSocket**.
  No durable replay, resume token or gap-fill → always snapshot-reconcile after reconnect.
- Account-state events arrive about every 2 s → compute live equity/P&L from ticks; use account events to correct drift.
- Orders: market via positions endpoint; pending types `limit`, `stop`, `sltp`; modify; cancel; close with
  `volume` for a partial close; `deviation` (slippage); close-by supported.
- Candles: S1, S5, S15, M1, M3, M5, M10, M15, M30, H1 and higher. History depth ≈ S1 1 day, M1 1 year, H1+ full.
  Count per request −1000…1000. No historical ticks → our own tick recorder matters.
- Rate limits come from a runtime limits endpoint (no published numbers) → read them at startup.
- Known error to map: `EXECUTION_REQUOTE` → `Requote`.
- API is new (v1.0.x, empty changelog): pin the spec version, add schema-drift tests, expect breaking changes.

### 5.2 cTrader Open API

- Docs: https://help.ctrader.com/open-api/ · Protos: https://github.com/spotware/openapi-proto-messages
  (vendor them into `brokers/ctrader/proto/`, compile with `protoc`, pin the commit).
- Endpoints: `demo.ctraderapi.com` / `live.ctraderapi.com`, **port 5035 = Protobuf**, 5036 = JSON.
  The same host and port serve TCP and WebSocket. **Use Protobuf over TLS TCP** (length-prefixed frames).
- Rate limits: **50 req/s non-historical, 5 req/s historical, per connection**.
- **Auth flow:** register the app (client ID + secret) → OAuth2 authorization code → access + refresh token →
  `ProtoOAApplicationAuthReq` → `ProtoOAGetAccountListByAccessTokenReq` → `ProtoOAAccountAuthReq` per account.
  Refresh with `ProtoOARefreshTokenReq` before expiry. Handle `ProtoOAAccountsTokenInvalidatedEvent`.
  Store tokens encrypted at rest.
- **Heartbeat:** send `ProtoHeartbeatEvent` about every 10 s when idle (the idle limit is ~30 s). Treat missing
  inbound traffic as a dead connection.
- **Units:** volume in **cents of units** (units × 100), and symbol min/max/step volumes are in cents too.
  Relative SL/TP in **1/100000 of a price unit**. Absolute prices must respect symbol digits, or the order is
  rejected. Spot and trendbar prices are integer-scaled (verify scale; trendbars use low + deltas).
  Trendbar volume is in ticks.
- Key messages: `ProtoOASymbolsListReq`, `ProtoOASymbolByIdReq`, `ProtoOATraderReq`, `ProtoOAReconcileReq`,
  `ProtoOANewOrderReq` (has `clientOrderId`, `label`, `comment`, `trailingStopLoss`, `relativeStopLoss/TakeProfit`),
  `ProtoOAAmendOrderReq`, `ProtoOACancelOrderReq`, `ProtoOAAmendPositionSLTPReq`, `ProtoOAClosePositionReq`,
  `ProtoOAExecutionEvent`, `ProtoOAOrderErrorEvent`, `ProtoOAErrorRes`, `ProtoOASubscribeSpotsReq`/`ProtoOASpotEvent`,
  `ProtoOAGetTrendbarsReq`, `ProtoOADealListReq`, `ProtoOAClientDisconnectEvent`.
- Native: SL/TP, trailing stop, partial close, hedging (account-dependent).
- Write our own asyncio transport; don't use the Twisted-based official Python SDK.
- Later: a separate `ctrader_fix` adapter (free FIX API per account) for lower-latency execution.

### 5.3 Paper broker (`brokers/paper/`)

- Uses a real adapter for market data only. Simulates fills on bid/ask with configurable slippage, latency,
  commission, swap and spread widening.
- Supports every capability, so strategies can run anywhere. It is also the seed of the future backtest simulator.

---

## 6. Order Management System (`src/trader/oms/`)

- **OrderManager** per account: submits, tracks state, applies events, owns retries (same `client_order_id`),
  timeouts → `UNKNOWN` → targeted reconcile.
- **Reconciliation** at startup, after reconnect and every N minutes: diff local vs broker snapshot.
  Broker wins. Every correction is logged as `ReconcileEvent` with a before/after for audit.
  Orphan positions (not created by us) are adopted as `strategy_id="manual"` and still count toward risk.
- **Bracket orders:** entry + SL + TP sent natively when capable, else attached right after the fill.
- **Emulated orders** (used when the broker lacks the capability): trailing stop, break-even move,
  OCO groups, time-based exit (close after N bars/minutes), scale-out ladders (TP1/TP2/TP3 partials).
  Emulation runs in the OMS on live ticks and survives restarts (state persisted).
- **Manual trading** commands (API/CLI): market/pending order, modify, close, close all for a symbol or strategy,
  **flatten account**. All of them go through risk.
- **Order tagging:** every order carries `strategy_id` + strategy version. Use the broker's label/comment fields
  where available, and always keep the local mapping `client_order_id → strategy`.

---

## 7. Risk management (`src/trader/risk/`)

### Pre-trade checks (every intent, all configurable per account and per strategy)
- Max volume per order; max total volume per symbol; max open positions (per strategy / symbol / account).
- Max gross and net exposure (account currency) and max margin usage % after the trade (estimate margin).
- **Max loss per trade** (requires an SL unless the strategy is explicitly whitelisted).
- **Daily loss limit** (realized + unrealized, account currency and %) and **max drawdown from equity peak**.
- Max orders per minute (runaway-loop guard) and max order-to-fill ratio.
- Spread filter (reject if spread > X × typical or > absolute cap) and stale-price guard (last tick age > T).
- Price sanity / fat-finger: limit/stop distance from market within bounds; SL on the correct side.
- Trading-session filter (instrument hours, rollover window, weekend) and optional blackout windows
  (manual news times).
- Duplicate-signal guard (same strategy, symbol, side within T seconds).

### Position sizing (`risk/sizing.py`)
Fixed lots · fixed money risk · % of equity risked per trade using SL distance and tick value ·
volatility-based (ATR) · caps by instrument min/max/step (round down, never up).

### Circuit breakers and kill switch
- **Kill switch** (API, CLI, automatic): stop all strategies, cancel pending orders, optionally flatten.
  Levels: `PAUSE_NEW` (no new entries), `CANCEL_PENDING`, `FLATTEN_ALL`.
- Automatic triggers: daily loss hit, drawdown hit, repeated rejects, feed stale, broker disconnected longer
  than T, reconcile mismatch, clock drift > threshold.
- While in breaker state, only risk-reducing orders (closes) are allowed.
- Every decision (approve/reject + reason + rule) emits a `RiskEvent` and is stored.

---

## 8. Strategy framework (`src/trader/strategy/`)

```python
class Strategy(ABC):
    id: str                  # unique instance id, e.g. "ema_cross_eurusd_m5"
    version: str             # bump on logic change; stored with every order
    Params: type[BaseModel]  # pydantic params model, validated on load

    def on_start(self, ctx: StrategyContext) -> None: ...
    def on_tick(self, ctx: StrategyContext, tick: Tick) -> None: ...
    def on_bar(self, ctx: StrategyContext, bar: Bar) -> None: ...
    def on_order(self, ctx: StrategyContext, ev: OrderEvent) -> None: ...
    def on_position(self, ctx: StrategyContext, ev: PositionEvent) -> None: ...
    def on_timer(self, ctx: StrategyContext, name: str) -> None: ...
    def on_stop(self, ctx: StrategyContext) -> None: ...
```

`StrategyContext` exposes: `now()`, `bars(symbol, tf, n)`, `last_tick(symbol)`, `indicator(...)`,
`positions()`, `orders()`, `account()`, `buy/sell/place(...)` (these return intents, never broker calls),
`modify`, `close`, `set_timer`, `state` (a persisted dict), `log`, `params`.

- **Subscriptions** are declared in config (symbols, timeframes). The bar builder makes bars from ticks for
  any timeframe and seeds history from `broker.candles()` at startup (warm-up period per strategy).
- **Multi-symbol, multi-timeframe** support; several instances of one strategy class with different params.
- **Lifecycle:** load → validate params → warm-up → `RUNNING` ↔ `PAUSED` → `STOPPED`. Hot start/stop/pause via API
  without restarting the process. Params change = new instance (old one stops cleanly first).
- **Isolation:** an exception in a strategy callback is caught, logged and counted; N errors → auto-pause that
  strategy only. A slow callback (> budget ms) is logged.
- **Persistence:** `ctx.state` is saved after each callback that changes it and restored on restart.
- **Indicators** (`strategy/indicators/`, incremental O(1) updates, tested against reference values):
  SMA, EMA, WMA, RSI, ATR, MACD, Bollinger Bands, Stochastic, ADX, Donchian, SuperTrend, VWAP (session),
  pivot points. Each has `update(bar|tick)` and `value`, and a `ready` flag after its warm-up.
- **Examples** in `strategy/examples/`: EMA crossover with ATR stops, breakout (Donchian) with time exit,
  RSI mean-reversion with scale-out. They exist to exercise the framework and contract tests.

---

## 9. Portfolio and P&L (`src/trader/portfolio/`)

- Positions kept current from fill/position events. Unrealized P&L recomputed on every tick, using
  contract size and quote→account currency conversion (via the conversion pair's live tick).
- **Realized P&L comes from broker deals** (source of truth), including commission and swap.
- Attribution per **account, strategy, symbol, day/session**. Gross vs net (after costs).
- Equity curve sampled every N seconds plus on every fill. High-water mark and drawdown tracked live
  (these feed risk).
- Drift check: computed equity vs broker-reported equity → alert if the gap exceeds a threshold.

---

## 10. Analytics (`src/trader/analytics/`)

- **Trade journal:** one record per round trip (entry fills → exit fills): strategy + version + params hash,
  entry/exit prices and times, SL/TP history, MAE/MFE (max adverse/favourable excursion from ticks), R multiple,
  costs, tags, free-text notes (added via API).
- **Performance metrics** per strategy/symbol/account and period: net P&L, win rate, avg win/loss, profit factor,
  expectancy, avg R, max drawdown (abs/%), recovery factor, Sharpe/Sortino (daily returns), longest
  win/loss streak, exposure time, trades per day.
- **Execution quality (top priority):** for every order record signal time, intent price, send time, ACK time,
  fill time and fill price. Derive **slippage** (points and money, positive/negative), **latencies**
  (signal→send, send→ack, ack→fill, total) as p50/p95/p99 per broker/symbol/hour, rejection and requote rate.
  Compare brokers side by side. This is how we prove the new system beats MT5.
- **Reports:** daily summary (P&L, trades, risk events, execution stats) via notification + stored JSON/CSV export.

---

## 11. Market data (`src/trader/marketdata/`)

- Tick bus with per-symbol last-tick cache. **Stale-feed monitor** per symbol (expected tick rate by session).
- Bar builder: tick → bars for configured timeframes, with exact boundary handling in UTC and broker
  session alignment. Mark bars complete/incomplete. Strategies get `on_bar` only for complete bars.
- **Tick recorder** (always on): append every received tick to Parquet files partitioned
  `data/ticks/{broker}/{symbol}/{yyyy-mm-dd}.parquet`, flushed in batches. This becomes the backtest dataset.
  Also store broker candles fetched for warm-up.

---

## 12. Persistence (`src/trader/persistence/`)

PostgreSQL tables (Alembic-managed): `accounts`, `instruments`, `strategies` (instances, params, version, status),
`orders`, `order_events` (every state transition), `fills`, `positions` (+ history), `account_snapshots`,
`risk_events`, `reconcile_events`, `trades` (journal), `strategy_state`, `audit_log` (every manual command:
who, what, when, result).

- **Event store:** append-only table of all domain events (except ticks) → audit and future replay.
- Writes are async (a background writer task with batching). A DB outage must not stop trading: buffer to a
  local WAL file and replay it, and alert.
- Ticks live in Parquet, not Postgres.

---

## 13. Control API (`src/trader/api/`) — consumed by the future UI

- REST: `/health`, `/accounts`, `/accounts/{id}/state`, `/positions`, `/orders`, `/orders` (POST manual order),
  `/positions/{id}/close`, `/strategies` (list/create/start/pause/stop/params), `/risk` (status, limits, kill switch),
  `/pnl`, `/trades`, `/analytics/execution`, `/analytics/performance`, `/audit`.
- WebSocket `/stream`: ticks (throttled per client), order/position/account/risk/strategy events.
- Auth: single-user bearer token or API key from env; bind to localhost by default, expose only behind a reverse
  proxy with TLS. Every mutating call goes to `audit_log`.
- OpenAPI schema generated by FastAPI is the contract for the UI.

---

## 14. Notifications (`src/trader/notify/`)

Telegram bot (primary) and email (optional). Events: fills, SL/TP hits, rejects, risk breaches,
kill switch, disconnect/reconnect, reconcile mismatches, daily report. Rate-limit and dedupe alerts.
The bot can optionally accept `/status`, `/pnl`, `/pause`, `/kill` commands (restricted to the allowed chat ID).

---

## 15. Operations

- **Modes:** `paper` (default) → `demo` → `live`. Live needs `TRADER_MODE=live` + `--confirm-live`, and prints a
  startup banner with account, broker and risk limits.
- **Config:** YAML per environment (`config/*.yaml`) for accounts, strategies and risk; secrets only from env or
  a secrets file with 600 permissions.
- **Startup sequence:** load config → connect DB → for each account: connect adapter → instruments → snapshot →
  reconcile → restore OMS/emulation/strategy state → warm up bars → start strategies.
- **Graceful shutdown** (SIGTERM): pause strategies → wait for in-flight orders to settle → persist state →
  disconnect. Never leave orders in `PENDING_SUBMIT` unrecorded.
- **Time sync:** run chrony/NTP on the host; measure local vs broker clock offset and alert on drift.
- **Observability:** structlog JSON logs (redacted), Prometheus metrics (latencies, tick rates, queue depths,
  order counts, rejects, reconnects, risk events), `/health` with per-account connection status.
- **Deployment:** Docker image + compose (app, Postgres). Run on a VPS close to the broker's servers.
  Health-check restarts are allowed only because startup reconciliation is safe.

---

## 16. Build phases (each ends with tests green and docs updated)

| # | Phase | Acceptance criteria |
|---|---|---|
| 0 | Scaffold: uv project, ruff/mypy/pytest, CI, config + logging + redaction, domain models | `uv run pytest` green; mypy strict clean |
| 1 | Broker interface, Capabilities, errors, registry, **PaperBroker** (driven by a fake tick source), contract suite | Contract suite green on paper |
| 2 | **cTrader adapter** (demo): proto vendoring, TLS transport, OAuth + token refresh, heartbeat, reconnect, mapping, rate limiter | Contract suite green on cTrader demo |
| 3 | **Exness adapter** (demo/Exness-platform account): signing, Access Point discovery, REST client from OpenAPI spec, server-events WS, ticks WS, ACK→event flow | Contract suite green on Exness |
| 4 | Market data: tick bus, bar builder, stale monitor, tick recorder | Bars match broker candles on the same window (tolerance documented) |
| 5 | OMS: state machine, submit/modify/cancel/close, timeouts→UNKNOWN, reconciliation, brackets, emulated orders | Property tests on state machine; restart-mid-order test passes |
| 6 | Risk engine: rules, sizing, kill switch, circuit breakers | Every rule has reject/approve tests; kill switch flatten works on demo |
| 7 | Strategy framework: base, context, runner, indicators, examples, state persistence, isolation | Example strategies run on paper + demo for 24h without errors |
| 8 | Portfolio & P&L, analytics (journal, metrics, **execution quality**), reports | P&L matches broker statement for a demo day (documented tolerance) |
| 9 | Persistence hardening: event store, async writer, DB-outage buffer | DB kill test: trading continues, data replayed |
| 10 | Control API + WebSocket stream + Telegram notifications | API contract tests; alerts fire |
| 11 | Ops hardening: Docker, metrics, health, graceful shutdown, chaos tests (drop connection, kill process mid-order) | Chaos suite green; runbook in `docs/runbook.md` |
| 12 | Setup audit: fresh clone, configs, CI, Docker; the code against CLAUDE.md and this spec; fix what is wrong | Audit report in `docs/audit/`; every finding fixed and tested, or listed as open with a reason |

Later: chart UI (reads the Control API), backtester (replays recorded Parquet ticks through the PaperBroker/simulator
with the same strategy code), MT5 bridge adapter, cTrader FIX adapter, more brokers.

---

## 17. Open questions to resolve during build (record answers in ADRs)

- Exness: exact signing scheme, Access Point cache TTL, the limits endpoint's response format, whether
  orders support a label/comment for strategy tagging.
- cTrader: spot-event price scale, token lifetime, behaviour of `clientOrderId` on duplicate submits.
- Symbol naming per account type (e.g. Exness suffixes) and the instrument contract sizes for metals and indices.
- Commission models per account type, for accurate net P&L in the paper broker.
