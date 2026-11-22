# ADR 0012 — Control API, live stream and notifications

Status: accepted (phase 10). Code: `src/trader/api/` (`schemas.py`, `control.py`, `routes.py`,
`stream.py`, `server.py`), `src/trader/notify/` (`notifier.py`, `rules.py`, `telegram.py`, `mail.py`,
`service.py`), `src/trader/persistence/audit.py`, wired in `trader.app.App`. Spec: SPEC §13, §14.
Guides: [`docs/api/README.md`](../api/README.md), [`docs/notifications.md`](../notifications.md).

Acceptance: **API contract tests; alerts fire.**
- `tests/integration/test_api.py`: every route over HTTP against the real `App` on paper, plus the
  committed OpenAPI contract.
- `tests/integration/test_stream.py`: the WebSocket.
- `tests/integration/test_notifications.py`: alerts through the real Telegram channel to a fake Bot API
  server, including `/kill` from Telegram.

## Decisions

### One facade for every operator: `Control`

The API, the Telegram bot (and later the UI) call `Control`, a thin facade over the running `App`. It
keeps the rules in one place:

- **The one order path.** A manual order is an `OrderIntent` with `strategy_id="manual"` submitted to
  `OrderManager.submit`. The RiskEngine checks it like any strategy order; `risk.strategies.manual` can
  limit it. A risk rejection is returned as a `REJECTED` order, not as an HTTP error. Closing a position is
  risk-reducing and allowed at any kill level.
- **The OMS stays the only writer of order state.** `Control` only calls OMS and runner commands.
- **Everything mutating is audited**, including refusals and failures: actor (`api`, `telegram`, `cli`),
  action, details, result. `AuditLog` appends to `data/audit/audit.jsonl` (0600, fsync per entry), so the
  trail exists without a database, and to `audit_log` when persistence is configured. `trader kill` now
  writes the local file too.

### In-process uvicorn, its signal handling disabled

The API runs in the trading process on the same event loop, so it reads live state with no IPC.
`uvicorn.Server.serve()` normally replaces the SIGINT/SIGTERM handlers and re-raises the signal after
shutdown. That would fight `trader run`'s own graceful shutdown, so `ApiServer` uses a `Server` subclass
whose `capture_signals` does nothing.

Lifecycle:
- **Start:** the API starts **last** in `App.start`, so it only ever sees a fully started app.
- **Stop:** it stops **first** in `App.stop`, so no commands arrive during shutdown.
- **Failed start:** a port that cannot be bound makes uvicorn call `sys.exit`. We turn that into a
  `ConfigError`, and `run_app` then stops the already-started app gracefully. Tested.

### Auth and exposure

- **Token:** a single bearer token from `TRADER_API_TOKEN`, at least 32 characters (checked at startup),
  compared with `hmac.compare_digest`. `GET /health` is the only open route and shows no details.
- **Bind:** `127.0.0.1` by default; a warning is logged for any other host. TLS is the reverse proxy's
  job.
- **Logging:** uvicorn's access log is off, because its lines carry query strings. Our request log
  records method, path, status and duration.
- **WebSocket token:** never in the URL. It comes from a header, or from the first message (browsers
  cannot set headers on a WebSocket).
- **`POST /strategies`:** accepts only built-in classes, or classes the config file already uses. An API
  call never makes the process import a module it was not configured with.

### Contract

- **OpenAPI:** the FastAPI schema is the UI contract, committed at `docs/api/openapi.json` and generated
  by `trader openapi`. A test fails when the file is stale.
- **Response models** are pydantic models in `api/schemas.py`. Money, prices and volumes are decimal
  strings.
- **Times are ISO-8601 UTC in the API** (REST and stream), not epoch ms. This is a deliberate exception
  to "epoch-ms on the wire": that rule is about brokers and storage. For a browser UI and its OpenAPI
  types, `date-time` strings are the standard format. Storage is unchanged:
  `persistence.serialize.to_json(iso_times=False)`.

### `/stream`

- **Fan-in:** a `StreamHub` merges every event bus (OMS per account, portfolio alerts, market feeds,
  strategies, the DB writer) into one bus.
- **Per client:** each client gets a bounded `DROP_OLDEST` subscription (2000) and its own `COALESCE` tick
  subscription, filtered to its symbols.
- **Timing:** events go out immediately. Ticks and account updates go out at most once per client
  interval (latest only). A slow client loses the oldest events and is sent `lagged` with the count, so
  trading never waits on a socket.

### Notifications

- **`Notifier`:** `notify()` is synchronous and never raises. Each channel has its own queue and worker.
  - **Dedupe:** by key within `dedupe_s`; the next notice says how many were folded in.
  - **Rate limit:** per channel per minute. Extras are counted and summarised. Critical notices are
    exempt, because a kill switch must always arrive.
  - **Retries:** 3 attempts, honouring `retry_after`.
- **`EventRules`:** turns domain events into notices (table in `docs/notifications.md`). It is stateful
  only where it must be:
  - last known positions, to tell an SL/TP hit from a manual close by price;
  - connection state, to say "reconnected" rather than "connected";
  - stale feeds, to say "feed back".
- **`TelegramChannel`:** plain-text `sendMessage` over httpx. Errors never include the URL, because the
  token is in the URL path. The log redaction gained a Telegram-token pattern.
- **`EmailChannel`:** smtplib in a worker thread with STARTTLS. Default minimum severity `warning`.
- **`TelegramBot`:** long-polls `getUpdates`. It accepts commands only from the configured chat, only if
  that chat is **private**, and only if the message is younger than 2 minutes, so a queued `/kill` is not
  replayed hours later.
  - `/kill` defaults to `PAUSE_NEW`; `flatten` must be typed.
  - The kill switch **cannot be reset from Telegram**: a lost phone must not be able to resume trading.

## Not verified: Telegram Bot API facts

The official Bot API documentation could not be fetched from the development environment. The facts the
code relies on are listed as **unverified assumptions** in `docs/notifications.md`, each mapped to a step of
the demo-marked probe `tests/contract/test_telegram_probe.py`. Run it with a real bot and record the
result there. The integration tests use a fake server implementing exactly those assumptions, so a wrong
assumption would show up in the probe, not in CI.

## Consequences

- **One process:** the API's availability is the trading process's. When it is down there is no API. That
  is acceptable: there is nothing to control.
- **API-created strategies** last until the process stops; the config file stays the source of truth for
  what runs after a restart.
- **SL/TP classification is a price heuristic.** A manual close right at the SL price reads "SL hit".
- **The audit file grows without bound** (one short line per command). Rotation can come with the
  deployment work in phase 11.
