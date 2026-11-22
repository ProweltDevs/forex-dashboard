# ADR 0013 — Ops hardening: metrics, health, Docker, chaos suite

Status: accepted (phase 11). Code: `src/trader/observability/` (`metrics.py`, `ops.py`, `http.py`),
`Dockerfile`, `docker-compose.yml`, `scripts/docker_smoke.sh`, wiring in `trader.app.App`.
Tests: `tests/chaos/`, `tests/integration/test_ops.py`, `tests/unit/test_metrics.py`,
`tests/unit/test_deploy_files.py`. Spec: SPEC §15. Operator guide: [`docs/runbook.md`](../runbook.md).

Acceptance: **chaos suite green** (`uv run pytest -m chaos`, also in CI); **runbook** in
`docs/runbook.md`.

## Decisions

### Ops endpoints on their own port, without auth

`/metrics`, `/health/live` and `/health` are served on `ops.port` (9108), separate from the Control API:
- Prometheus and Docker health checks need no token.
- They work whether or not the API is enabled.
- They carry no secrets.

They do show account status, so they bind to `127.0.0.1` (or `0.0.0.0` inside a container that publishes
the port on the host's `127.0.0.1` only; a test checks the compose file).

The ops server **starts first and stops last**, so:
- liveness answers during a long warm-up;
- readiness reports `starting` and `stopping` truthfully.

### Liveness for restarts, readiness for alerts

- **`/health/live`** answers while the process serves requests. The Docker `HEALTHCHECK` uses it: a hung
  process gets restarted, which is safe because startup always reconciles with the broker before
  anything trades.
- **`/health`** is 200 only when every account is connected and reconciled.
  - `degraded`, still 200: something needs a look (kill switch on, stale feed, database down).
  - 503: `starting`, `stopping` or `not_ready`.

  Restarting on readiness would be wrong: a broker outage is not cured by a restart.

### Metrics

- **Registry:** `prometheus-client` with its own `CollectorRegistry` per `App`, with no global state.
- **Collection:** `OpsMonitor` consumes the event buses through bounded `DROP_OLDEST` subscriptions
  (metrics must never hold memory or slow trading), and samples gauges every `ops.sample_s`.
- **What is measured:** the SPEC §15 list (latencies, tick rates, queue depths, order counts, rejects,
  reconnects, risk events) plus equity, drawdown, kill level, strategy status, database and
  notification health, and event-loop lag. The full list is in the runbook.
- **Order latency** comes from the order's own status history (`PENDING_SUBMIT` → `ACCEPTED` →
  `FILLED`), so it costs nothing extra. Fill legs are recorded for market orders only, because a limit
  order's resting time is not latency.

### Clock offset

For each feed, the receive time minus the broker's tick timestamp. The **minimum over a minute**
approximates the true offset plus the smallest network latency. Beyond `ops.clock_drift_alert_s` (2 s)
an `AlertEvent("clock_drift")` goes out through notifications, persistence and the stream. It clears
below half the limit. The host still needs chrony; this only detects when it is not doing its job.

### Docker

- **Build:** multi-stage, with `uv sync --locked --no-dev` into `/app/.venv`. The runtime stage has no
  build tools and runs as an unprivileged user (uid 10001).
- **Data:** the data directory is a volume: it is the process's memory.
- **Health and signals:** `HEALTHCHECK` on liveness; `STOPSIGNAL SIGTERM`; compose
  `stop_grace_period: 60s` (orders settle for 10 s, then the flush); `init: true`;
  `restart: unless-stopped`; log rotation.
- **Base image as a build arg:** a mirror can stand in when Docker Hub rate-limits. That happened while
  building this phase; `mirror.gcr.io` worked.
- **Build CA as a BuildKit secret:** an optional CA for TLS-intercepting proxies is mounted at build
  time only, so it never reaches the image.
- **`TRADER_MODE` pinned empty in compose.** `env_file: .env` turns `.env` into the process
  environment, which would let a `TRADER_MODE=live` line in `.env` count, defeating one of the three
  live gates. Only an explicit `docker-compose.live.yml` override sets it. Found while writing the
  runbook; guarded by `tests/unit/test_deploy_files.py`.
- **Smoke test in CI:** `scripts/docker_smoke.sh` builds the image, starts PostgreSQL and the app, waits
  for Docker `healthy` and `/health` `ready`, stops it with SIGTERM and requires exit code 0 after
  `app_stopped`.

### Broker unreachable at startup: exit and let the supervisor retry

`trader run` stops what it had started and exits with code 2. Docker (`restart: unless-stopped`) or
systemd retries with growing delays. This keeps startup simple and every start identical (connect →
reconcile → trade).

A broker lost while running is different: the adapter reconnects by itself and the OMS reconciles
before strategies resume.

The chaos suite tests both.

## The chaos suite

`tests/chaos/` runs a real `trader run` (`python -m trader.cli`) against the fake cTrader server, which
runs in the test process and so outlives the trader. Kills are real signals.

- **Fake broker knobs:** `ack_delay_s` (execute later, after the trader is gone), `swallow_orders` (a
  request lost on the way in), `go_down` and `come_back` (refuse connections on the same port, state
  kept).
- **Answers to dead connections:** like the real server, they go to the account's current sessions.
- **The invariant after every scenario:** each order executed by the broker exactly once, none lost,
  none left in flight, and the trader's positions equal the broker's.
- **Safety:** the process gets a throwaway env file and an environment with every `TRADER_`,
  `CTRADER_`, `EXNESS_` and `TELEGRAM_` variable removed. It cannot pick up real credentials.

| Scenario | Result |
|---|---|
| SIGTERM with an order in flight | exit 0; the order settles and is persisted FILLED; report written |
| SIGKILL with 3 orders in flight (one lost on the way), restart | 2 found executed and adopted; the lost one resent once with the same `client_order_id`; no duplicate |
| TCP reset with an order in flight | reconnect, reconcile finds it (no resend), trading resumes |
| Broker unreachable, then back | 503 `not_ready`; manual orders refused with 503, never queued; recovery |
| Broker down at startup | exit 2 after a clean stop; the next start recovers |
| Notifications down | trading unaffected |
| PostgreSQL killed (`test_db_kill.py`, phase 9) | trading continues; WAL replayed; database equals books |

All of them passed on the first full run except the "unreachable" scenario. It hung on a test-infra bug:
in Python 3.12 `Server.wait_closed()` waits for open connections, so the fake must stop accepting, drop
connections, then wait. It was fixed in the fake. No production code needed changing for the chaos
scenarios: the restart and reconcile design from phases 5 and 9 held.

## Consequences

- **One broker down at startup stops a multi-account process** until it is back. If that ever matters,
  the next step is a degraded start: accounts start independently and an unreachable one keeps
  retrying.
- **Readiness depends on every account,** so one flaky broker makes the whole process `not_ready` for
  monitoring. `/health` names the account.
- **The image is about 550 MB,** mostly pyarrow (the tick recorder).
- **Metric label sets** (account, strategy, symbol, rule) are bounded by the config. Reject `rule`
  labels are cut to a stable prefix.
