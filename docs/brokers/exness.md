# Exness Trader API — verified facts

Adapter: `src/trader/brokers/exness/`. Design: [ADR 0005](../adr/0005-exness-adapter.md).
Source of truth: OpenAPI spec **v1.0.2** (<https://www.exness-api.com/openapi/trading.public.yaml>) and the
guides under <https://www.exness-api.com/trading/>, read on 2026-11-22. The changelog pages were empty.
**V** = verified in the spec/guides. **A** = assumption, with the demo probe that settles it
(`uv run pytest -m demo tests/contract/test_exness_probes.py -s`).

> The API works only with **Exness-platform** accounts (not MT4/MT5). The host does not separate demo from
> real accounts, so the demo tests refuse to run unless `EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes`.

## Hosts and credentials

| | Fact | Source |
|---|---|---|
| V | Keys are created in Personal Area → **Key Management**. Trading **Full access** covers exactly one account; Read-only covers 1–10. | [api-key-and-host-setup](https://www.exness-api.com/authentication/api-key-and-host-setup) |
| V | The private key is either **128 hex chars** (32-byte seed + 32-byte public key, issued by Key Management) or an **unencrypted PKCS#8 PEM** (self-generated; register the SPKI DER public key as padded Base64). | same |
| V | The **DNS widget** in Key Management shows the **Gateway** host. `GET {gateway}/v1/trading/access-point?account_id=…` (signed) returns `{"access_point": "https://…", "account_id": "…"}`. All account REST calls and WebSockets go to the Access Point. Discovery is rate limited: cache it. Example hosts in the docs are not fallbacks. | [access-point-host-discovery](https://www.exness-api.com/trading/get-started/access-point-host-discovery) |
| V | Set a meaningful `User-Agent`. An HTML/plain-text 403 comes from an edge/Gateway layer, not the API. | [troubleshooting](https://www.exness-api.com/trading/guides/troubleshooting) |

## Request signing (checked against the official test vector in `tests/unit/brokers/test_exness_units.py`)

| | Fact | Source |
|---|---|---|
| V | Headers `EXN-API-KEY`, `EXN-IDEMPOTENCY-KEY`, `EXN-TIMESTAMP` (ms), `EXN-SIGN-VERSION: 1`, `EXN-DATA`, `EXN-SIGN`. | [request-signing](https://www.exness-api.com/authentication/request-signing) |
| V | `EXN-DATA` = base64url (no padding) of the JSON `{api_key, idempotency_key, timestamp, sign_version, method, path, body_hash}`. `EXN-SIGN` = Ed25519 over **those exact bytes**. `body_hash` = base64url SHA-256 of the exact body (empty body → `47DEQpj8…uFU`). | spec `info`, same |
| V | `path` includes the query exactly as sent (no reordering or re-encoding). Duplicate query params are rejected. The timestamp must be close to server time. | same |
| V | Signed GETs send `EXN-IDEMPOTENCY-KEY` **present but empty**. Mutating requests need a 5–128 char key `[A-Za-z0-9._~-]`. | same |
| V | WebSocket handshakes are signed as `GET` of the exact connection path. Do not follow redirects with signed headers. | [subscribe-to-websocket-events](https://www.exness-api.com/trading/get-started/subscribe-to-websocket-events), code examples |
| A | The WebSocket URL is `wss://{access point host}{path}`, offered with subprotocol `exness-ws-protocol`. The docs' helper is marked "URL construction not yet verified". Probe: `test_probe_streams_limits_and_naming`. | [code-examples](https://www.exness-api.com/trading/guides/code-examples) |

## Trading model

| | Fact | Source |
|---|---|---|
| V | **Market orders** = `POST /v1/trading/accounts/{id}/positions` `{instrument, side, volume, price?, deviation?, stop_loss_price?, take_profit_price?, comment?}`. `deviation` (points) applies only when `exec_mode = instant` and `price` is given. | spec `openPosition` |
| V | **Pending** = `POST …/orders` with `type` `limit` / `stop` (no market, **no stop-limit**, **no expiry/TIF field**). `type: sltp` + `position_id` attaches SL/TP to a position. | spec `placePendingOrder` |
| V | `PATCH …/orders/{id}` changes `price`, `stop_loss_price`, `take_profit_price` only (omitted = unchanged, `null` = remove). `DELETE …/orders/{id}` cancels a pending or SLTP order. | spec `modifyOrder`, `cancelPendingOrder` |
| V | `DELETE …/positions/{id}?volume=` closes (partial if volume given). `close_by_position_id` exists. | spec `closePosition` |
| V | All mutating calls return **202 ACK** `{operation_id, client_request_id, status: accepted}`. The result arrives as a `transaction_event` (`success` / `rejected` / `failed` + `error_code`/`error_message`). Status can be polled at `GET …/operations/{op}` (kept 24 h). | spec, [runtime-semantics](https://www.exness-api.com/trading/guides/runtime-semantics) |
| V | **Idempotency:** the same key + operation + payload returns the original `operation_id` and is not executed again. The same key with a different payload is a conflict. The key is echoed as `client_request_id` in the ACK, the event and the status. Records expire after a limited time. | spec `info` |
| V | Orders, positions and deals do **not** echo the comment or client id. Correlation comes only from the transaction's `client_request_id`, so the adapter keeps `client_order_id → order/position` locally. | spec schemas |
| V | Volumes are **lots** as decimal strings (`volume_min`/`max`/`step` from instrument conditions; `contract_size` e.g. `100000`). Request prices allow at most `point_digits` decimals. Trading prices and money are decimal strings; ticks and candles are JSON numbers. | spec `info`, `InstrumentConditionResponse` |
| V | Positions carry no SL/TP: they are on a linked `sltp` order. In the snapshot a pending order's own SL/TP sits on the order. | spec `WSTradingTransactionEvent`, `Order` |
| V | Deals: types `open`/`close`/balance types; `profit {amount, currency}`, `commission`. There is no swap field on deals (positions carry accumulated `swap`). | spec `Deal`, `Position` |
| V | **History:** `GET /v1/history/accounts/{id}/orders` (closed orders only: `filled`/`cancelled`/`rejected`; open and placed orders come from the snapshot) and `…/deals` (`deal_type` filter: `open`, `close`, balance types). Query `from` (inclusive, required without `cursor`), `to` (exclusive, optional), `limit` 1–200 (default 100), `instrument`; `order_state` for orders. A `cursor` cannot be combined with filters other than `limit`. Response `{items: [{event_time, order|deal}], next_cursor, has_more, range}`, ascending by `event_time`. Window and age limits come from `…/limits`. | spec `getOrdersHistory`, `getDealsHistory` |
| V | `Order` has no field that tells an opening market order from a closing one; `Deal.type` (`open`/`close`) and `Deal.order_id` ("if the deal was created as a result of an order execution") do. | spec `Order`, `Deal` |
| A | **Restart re-link** (`ExnessBroker._recover_operation`, ADR 0007): a confirmed operation without a `transaction_event` (a resend de-duplicated after a restart) is linked to the single untagged match on instrument, side and volume since the operation was created. A market open is matched through its `open` deal's `order_id`; a pending order through the snapshot or order history (same type and price). This relies on open deals carrying `order_id`. Probe: `test_probe_restart_relink_from_history`. | — |
| A | Commission is reported as a negative number (debit), as in the spec examples. Probe: `test_probe_close_deal_fields`. | — |
| A | Accounts are **hedging** (opposite positions coexist). Probe: `test_probe_sltp_visibility_and_hedging`. | — |
| A | The REST snapshot lists positions' active `sltp` orders, so SL/TP show after reconnect. Same probe. | — |
| A | `comment` is truncated to 31 chars (MT5 convention; the length limit is undocumented). | — |

## Streams

| | Fact | Source |
|---|---|---|
| V | `/v1/server-events/accounts/{id}/ws/events`: subscribe `{"id", "subscribe": {"event": "transactions"}}` → first message `trading_state_snapshot`, then `transaction_event`s. `account_state` → `account_state_event` every **2 s** (shared global timer). Also `instruments`, `hmr`. | spec `getWSEvents`, runtime-semantics |
| V | `/ws/ticks`: `{"id", "subscribe": {"event": "ticks", "instruments": [...]}}` → the last tick per instrument, then updates (may be throttled to 1 per **500 ms**). Tick = `{instrument, bid, ask, timestamp}`. | spec `getWSTicks` |
| V | **No durable replay, resume or gap-fill.** On reconnect: resubscribe and take the new snapshot as the baseline. If an ACKed `operation_id` never shows up, poll operation status, then resubscribe and reconcile. A slow reader can be disconnected. | spec `getWSEvents` |
| V | WS errors: `{id, code, error_message}`. | spec `WsErrorResponse` |

## Limits

| | Fact | Source |
|---|---|---|
| V | `GET …/limits` returns configured (not remaining) limits: `rest.global_account_rate {limit, window_seconds}` across the AP cluster, `rest.methods[] {api_operation_id, rate_limit, limits{max_candles_per_request, history_range_days, history_age_days}}`, and WebSocket limits per server node. Don't hardcode. Count locally. | [api-configuration-and-limits](https://www.exness-api.com/trading/guides/api-configuration-and-limits) |
| V | `REQUEST_RATE_LIMIT` (3015) = endpoint quota, HTTP 429, "non-retryable automatically". `SYSTEM_RATE_LIMIT` (1) = platform throttling. `Retry-After`/`retry_after` may be present. | spec `ErrorResponse`, troubleshooting |
| V | Candles: `from` required; `from+to` or `from+count` (±1000, configurable). `to` is exclusive. Range ≤ max candles. Depth: S1 1 d, S5 5 d, S15 15 d, M1/M3 1 y, M5/M10 2 y, M15/M30 3 y, H1+ full. Timeframes S1…MN (incl. H2). | spec `getCandleHistory` |

## Errors (mapped in `mapping.py`)

`EXECUTION_REQUOTE` → `Requote` · `TRADING_RULE_INSUFFICIENT_MARGIN`/`MARGIN_LEVEL_TOO_LOW` → `InsufficientMargin` ·
`TRADING_RULE_*VOLUME*` → `InvalidVolume` · `TRADING_RULE_INVALID_PRICE_LEVELS`, `REQUEST_INVALID_PRICE` → `InvalidPrice` ·
`MARKET_TRADE_DISABLED`/`CLOSE_ONLY`/`SESSION_CLOSED`/`NO_QUOTES` → `MarketClosed` · `*_RATE_LIMIT` → `RateLimited` ·
`AUTH_*` → `AuthExpired` · `SYSTEM_TEMPORARY_UNAVAILABLE`/`INTERNAL_ERROR` → `BrokerUnavailable` ·
`REQUEST_ORDER_NOT_FOUND` → `OrderNotFound` · `REQUEST_POSITION_NOT_FOUND` → `PositionNotFound` ·
`MARKET_INSTRUMENT_NOT_FOUND` → `UnknownSymbol` · other trading rules → `OrderRejected` · unknown → `BrokerError`.
The troubleshooting page also lists `AUTH_RATE_LIMIT` (1003), which is missing from the spec enum; it is mapped anyway.

## Setup (demo)

1. Create a **demo** Exness-platform account, then an API key in Key Management (Trading → Full access, that
   account). Store the API key identifier and the private key. Copy the Gateway host from the DNS widget.
2. In `.env`: `EXNESS_DEMO_API_KEY`, `EXNESS_DEMO_PRIVATE_KEY` (hex) or `EXNESS_DEMO_PRIVATE_KEY_FILE` (PEM path),
   `EXNESS_DEMO_ACCOUNT_ID`, `EXNESS_DEMO_GATEWAY_URL`, and `EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes`. Set
   `EXNESS_DEMO_SYMBOL_SUFFIX` if your instruments are named e.g. `EURUSDm`.
3. `uv run pytest -m demo` runs the contract suite and probes. Record the probe output here.

## Open questions (settle with the probes, then update this page)

- WebSocket URL scheme and subprotocol; tick `timestamp` format.
- Whether the REST snapshot includes positions' `sltp` orders; whether open deals carry `order_id` (the
  restart re-link depends on it: `test_probe_restart_relink_from_history`).
- Commission sign; hedging; comment length limit; instrument naming per account type.
