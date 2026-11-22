# ADR 0005 — Exness adapter

Status: accepted (phase 3). Protocol facts and their sources: [docs/brokers/exness.md](../brokers/exness.md).

## Structure

`auth.py` (Ed25519 signer, key loading) · `rest.py` (signed httpx client, error parsing, limits) ·
`discovery.py` (Gateway → Access Point) · `ws.py` (signed WebSocket stream) · `mapping.py` (pure conversions) ·
`adapter.py` (`ExnessBroker`). The models are hand-written from spec v1.0.2 rather than generated: the API
surface is small and the domain mapping is the real work. `docs/brokers/exness.md` pins the spec version.

## Decisions

- **Signing** follows the spec byte for byte: compact JSON, serialise once, sign the same bytes. It is unit-tested
  against the official test vector, with both key formats. The query string is built by us (strict
  percent-encoding, `Z` timestamps so no `+`). httpx is given the finished URL, and a test asserts that the
  transmitted path equals the signed one. Redirects are never followed.
- **Idempotency.** `client_order_id` (ULID, valid charset) is the idempotency key of the place request. Other
  mutating calls get a fresh ULID per call. A retry of `place_order` with the same `client_order_id` resends,
  the server returns the original `operation_id`, and we report `duplicate=True`.
- **ACK vs result.** `place_order` emits `ACCEPTED` at ACK (no broker order id yet, because the API only returns an
  `operation_id`). Fills, `WORKING`, `FILLED` and `REJECTED` come from `transaction_event`s, correlated by
  `client_request_id`. The pending-request entry is registered *before* sending, so an event that beats the
  ACK is still matched. A REST timeout after sending raises `UnknownOrderState`.
- **Missed events.** A watchdog polls `GET /operations/{op}` for ACKed operations without an event after
  15 s: `rejected`/`failed` → `REJECTED`; `confirmed` → the event was missed → drop and resubscribe the events
  stream, which (like every reconnect) gates trade events until the OMS takes a snapshot.
- **Attribution.** Entities echo neither the comment nor the client id, so the adapter keeps
  `client_order_id → order_id` and `position_id → strategy_id` in memory. After a restart, positions not
  re-attributed by the OMS show as `manual` until the persistence layer (phase 9) stores the mapping.
  The comment `strategy|version` (≤ 31 chars) is still sent for visibility in Exness's own UI.
- **SL/TP.** Market and pending orders send `stop_loss_price`/`take_profit_price` directly. For an open position,
  `modify_position` creates (`POST type=sltp`), changes (`PATCH`), or removes (`DELETE`) the linked `sltp` order.
  Position SL/TP views come from that order.
- **Capabilities.** MARKET, LIMIT, STOP; no STOP_LIMIT; no server-side expiry (GTD/DAY are rejected so the OMS
  emulates them); pending modify = price/SL/TP (volume changes are rejected); partial close yes; close-by not
  exposed yet; hedging per config (default true). `max_slippage` maps to `price` + `deviation` only on
  `exec_mode = instant` instruments; on `market` execution it is up to the OMS to check slippage after the fill.
- **Rate limits.** Exact sliding windows (`SlidingWindow`), account-wide plus per operation, loaded from
  `/limits` at every (re)connect, with a 5/s bootstrap until then. Candle requests are chunked by the
  published `max_candles_per_request`.
- **Streams.** The events stream subscribes to `transactions` and `account_state`. Its loss means DOWN, a
  reconnect with backoff and jitter, Access Point re-resolution if it fails, and a new snapshot as the
  baseline. Ticks-stream loss alone means DEGRADED (no trade state is lost), then UP without re-gating once
  restored.
- **Symbols.** Canonical names come from instrument names via an optional suffix (`symbol_suffix: m`) or
  aliases. Instrument conditions are loaded for configured symbols only (one request each).
- **Safety.** Exness's hosts don't distinguish demo from real, so the demo contract tests and probes also need
  `EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes`.

## Testing

`tests/integration/exness_fake.py` serves REST through `httpx.MockTransport` and WebSockets on a local
`websockets` server. It **verifies every signature** with the account's public key and models discovery,
limits, ACK→event, idempotency keys, sltp orders, partial close, the candle limit, per-operation rate limits,
and a "dropped event" switch. The contract suite runs on it (`exness-fake`) and on a real demo account with
`-m demo` (`exness-demo`).
