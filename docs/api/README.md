# Control API

The REST + WebSocket API the future UI (and scripts) use to watch and control the running `trader run`
process (SPEC §13, [ADR 0012](../adr/0012-control-api-and-notifications.md)).

- **Contract:** [`openapi.json`](openapi.json), generated from the code. A test fails when it is stale;
  regenerate it with `uv run trader openapi --out docs/api/openapi.json`. The WebSocket `/stream` is not
  part of OpenAPI; its protocol is below.
- **Served by** uvicorn inside the trading process (same event loop), when `api.enabled: true`.
- **Bind:** `127.0.0.1:8700` by default. To reach it from elsewhere, put a reverse proxy with TLS in front
  of it. Never expose the plain port.

## Auth

One bearer token, `TRADER_API_TOKEN` (`.env`, at least 32 characters):

```bash
curl -H "Authorization: Bearer $TRADER_API_TOKEN" http://127.0.0.1:8700/accounts
```

Every route except `GET /health` needs it; otherwise the answer is `401` with `WWW-Authenticate: Bearer`.
The token is compared in constant time. Request logs record method, path, status and duration; never the
query string or headers.

## Conventions

- Money, prices and volumes (lots) are **decimal strings** (`"1.08113"`), never floats.
- Times are ISO-8601 UTC (`"2026-11-22T17:04:38.007477Z"`).
- Errors are `{"detail": "..."}`:

| Status | Meaning |
|---|---|
| 401 | missing or wrong token |
| 404 | unknown account, order, position, strategy or trade |
| 409 | not possible in the current state (resume under a kill switch, pause a strategy that is not running) |
| 422 | invalid input (body, query, params a strategy rejects, an order the domain refuses) |
| 502 / 503 | broker error / broker unavailable |

- **Every mutating call is audited** (`GET /audit`): actor `api`, action, details, result. Refusals and
  failures too.

## Endpoints

| Method | Path | What |
|---|---|---|
| GET | `/health` | `ok`/`degraded`, connected accounts (no auth, no details) |
| GET | `/accounts` | accounts: broker, environment, connected, ready, kill level |
| GET | `/accounts/{id}/state` | balance, equity (broker and computed), margin, P&L today, drawdown, risk triggers |
| GET | `/positions?account=&symbol=` | open positions with unrealized P&L |
| POST | `/positions/{id}/close` | close (or `volume` lots of) a position; allowed at any kill level |
| GET | `/orders?account=&open=&strategy=&limit=` | orders, newest first |
| GET | `/orders/{client_order_id}` | one order |
| POST | `/orders` | **manual order**, see below |
| POST | `/orders/{client_order_id}/cancel` | cancel a pending order |
| GET / POST | `/strategies` | list / load a strategy instance |
| GET | `/strategies/{id}` | one strategy: status, params, stats |
| POST | `/strategies/{id}/start` | start a loaded or stopped strategy, resume a paused one |
| POST | `/strategies/{id}/pause` | pause (body `{"reason": ...}`) |
| POST | `/strategies/{id}/stop` | stop (`on_stop` runs, state is saved) |
| PUT | `/strategies/{id}/params` | new params: a new instance with the old state; a paused one stays paused |
| GET | `/risk?account=` | kill level, active triggers, limits and breakers |
| POST | `/risk/kill` | kill switch: `{"account": "<id>" or "*", "level": "PAUSE_NEW"/"CANCEL_PENDING"/"FLATTEN_ALL", "reason": ...}` |
| POST | `/risk/reset` | clear the manual switch and latched breakers |
| GET | `/pnl?account=&day=` | realized P&L of a trading day by strategy and symbol, plus equity |
| GET | `/trades?account=&strategy=&symbol=&since=&limit=` | closed trades, newest first |
| POST | `/trades/{id}/notes` | add a journal note |
| GET | `/analytics/execution?by=symbol,strategy` | slippage, latency, fill/reject/requote rates |
| GET | `/analytics/performance?by=strategy,symbol` | trade metrics (net, win rate, profit factor, drawdown, ...) |
| GET | `/audit?limit=&action=&account=` | audit log, newest first |
| WS | `/stream` | live events and ticks |

### Manual orders

`POST /orders` builds an `OrderIntent` with `strategy_id: "manual"` and submits it through the
**RiskEngine and the OMS**, exactly like a strategy order. Nothing bypasses risk. `risk.strategies.manual`
in the config can set limits for manual orders. The default limits require an SL.

```json
{"account": "paper-main", "symbol": "EURUSD", "side": "BUY", "volume": "0.01", "sl": "1.0750"}
```

The answer is the order. A risk rejection is not an HTTP error: it is an order with `status: "REJECTED"`
and `rejection_reason: "risk:<rule>: <why>"`.

### Strategies created through the API

`POST /strategies` takes the same fields as a `strategies:` config entry. The class must be a built-in
(`ema_cross`, `donchian_breakout`, `rsi_reversion`) or a class the config file already uses: an API call
never makes the process import a module it was not configured with. Such a strategy lasts until the
process stops; add it to the config file to keep it.

## `/stream` (WebSocket)

1. **Auth:** an `Authorization: Bearer <token>` header on the upgrade, or, from a browser (which cannot
   set headers), send `{"op": "auth", "token": "..."}` as the first message within 5 s. The token never
   goes in the URL, because URLs end up in proxy logs. A wrong token closes the socket with code `1008`. A
   header that is present must be right: there is no fallback to the message.
2. The server sends `{"type": "hello", "topics": [...]}`.
3. Subscribe (any time, to change it):
   `{"op": "subscribe", "topics": ["orders", "fills", "ticks"], "symbols": ["EURUSD"], "tick_interval_ms": 250}`.
   Default: every event topic, no ticks. The answer is `{"type": "subscribed", ...}`; a bad message gets
   `{"type": "error", ...}`.

Topics: `orders` (OrderEvent, OrderRetryEvent), `fills`, `positions`, `account`, `connection`, `feed`,
`reconcile`, `risk` (RiskEvent, RiskStateEvent), `strategies`, `alerts`, `ticks`.

Messages:

```json
{"type": "event", "topic": "fills", "kind": "FillEvent", "account_id": "paper-main", "data": {"fill": {...}, "ts": "..."}}
{"type": "ticks", "data": [{"feed": "paper-main", "symbol": "EURUSD", "bid": "1.08110", "ask": "1.08113", "ts": "..."}]}
{"type": "lagged", "dropped": 12}
```

- Order, fill, position, risk and other events are sent **as they happen**.
- Ticks and account updates are **throttled per client**: at most one batch per `tick_interval_ms`
  (minimum 50), with only the latest tick per (feed, symbol) and the latest state per account.
- **Backpressure:** each client has a bounded queue (2000 events). A client too slow to keep up loses the
  oldest events and receives `lagged` with the count. Trading never waits on a client.
- At most 32 clients (`1013` beyond that). At shutdown the socket closes with `1001`.
