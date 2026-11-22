# ADR 0004 — cTrader adapter

Status: accepted (phase 2). Protocol facts and their sources: [docs/brokers/ctrader.md](../brokers/ctrader.md).

## Structure

- `proto/` holds the vendored `.proto` files (pinned commit) and the generated `*_pb2.py`/`.pyi`
  (protoc via grpcio-tools, mypy-protobuf stubs). They are excluded from ruff and from strict mypy, and the stubs
  type-check our callers. Imports are rewritten to package-relative by `scripts/gen_ctrader_proto.sh`.
- `transport.py`: our own asyncio transport, not the Twisted SDK. TLS stream with 4-byte length framing.
  Requests get a `clientMsgId` and the first inbound message with that ID resolves them. Every inbound message
  also goes to the adapter's handler. It sends heartbeats every 10 s and declares the connection dead after 30 s
  without inbound data. Client-side token buckets are set a little under the documented limits (40/s, burst 10;
  4/s, burst 2 for historical requests). An optional HTTP CONNECT proxy is supported for networks that need one.
- `mapping.py`: pure, tested conversions (units, prices, trendbars, sessions, errors, orders, positions).
- `oauth.py`: grant URL, code exchange (REST) and an encrypted 0600 token store.
- `adapter.py`: `CTraderBroker` implementing the `Broker` protocol.
- `auth_cli.py`: `trader ctrader-auth`, which writes tokens to `.env` without printing them.

## Decisions

- **Outcome of a request.** A timeout, or a connection lost after the request was written, raises
  `UnknownOrderState` and never "failed". The OMS reconciles (SPEC §3).
- **ACK = first message with our `clientMsgId`.** For trading requests that is a `ProtoOAExecutionEvent`
  (usually `ORDER_ACCEPTED`) or an error event. Fills arrive later as events (`sync_order_result=False`).
- **Idempotency.** cTrader's `clientOrderId` is not documented as an idempotency key, so the adapter
  deduplicates locally: a known `client_order_id` returns the original order ID (`duplicate=True`), and a
  concurrent retry waits on the in-flight request. After a restart, the OMS reconcile (positions and orders
  carry `clientOrderId`/label) is the safety net.
- **Strategy tagging.** `label = "<strategy_id>|<version>"` (max 100 chars). Positions and orders without a
  label are attributed to `manual` (SPEC §6 orphan adoption).
- **Market SL/TP.** Absolute SL/TP are not allowed on MARKET orders, so they are sent as relative distances from
  the current quote. The resulting levels can shift by the fill slippage. `max_slippage` maps to `MARKET_RANGE`
  with `slippageInPoints`.
- **STOP_LIMIT** is sent as `stopPrice` + `slippageInPoints = |limit − stop| / tick` (to be probed).
- **Account state.** Equity = balance + Σ `netUnrealizedPnL`; margin = Σ `usedMargin`. Updates are emitted on
  balance-changing events and polled every 5 s while positions are open (the portfolio will compute
  tick-level equity in phase 8).
- **Internal orders.** Closing orders and SL/TP protection orders are not reported as `OrderEvent`s. Their
  fills and the position changes are.
- **Reconnect.** Exponential backoff (0.5 s × 2ⁿ, capped at 30 s, ±20 % jitter). After reconnecting the adapter
  re-authenticates, refreshes tokens if due, resubscribes spots, emits `UP`, and gates trade events until
  `snapshot()`, as the paper broker does. `ProtoOAAccountsTokenInvalidatedEvent` forces a token refresh on the
  reconnect. Client and account disconnect events trigger a reconnect.
- **Tokens.** Env vars bootstrap the first connection. With unknown expiry we refresh at once to learn it,
  and then refresh 7 days before expiry. Refreshed pairs are persisted encrypted (`TRADER_SECRETS_KEY`, Fernet);
  without a key they live in memory only, with a warning.
- **Sessions.** `ProtoOASymbol.schedule` (seconds from Sunday 00:00 in `scheduleTimeZone`) is converted into
  weekly UTC windows split at midnight, for the week the instruments are loaded (DST-aware).
- **Not exposed yet:** native trailing stop (`OrderRequest` has no field for it), removing SL/TP from a
  pending order, close-by, depth quotes, historical ticks.
- **No broker-name branching.** The cTrader-only config check (numeric `ctidTraderAccountId`) lives in the
  registry factory, not in the shared config model.

## Testing

- `tests/integration/ctrader_fake.py` is an in-process server speaking the real framing and messages: auth
  order, token rotation, execution events, reconcile, P&L, trendbars with a range limit, and a 50 req/s limit.
- The contract suite runs against it (`ctrader-fake`) on every test run, and against a real **demo** account
  with `-m demo` (`ctrader-demo`, skipped without credentials).
- Demo probes (`tests/contract/test_ctrader_probes.py`) settle the assumptions listed in the broker doc.
